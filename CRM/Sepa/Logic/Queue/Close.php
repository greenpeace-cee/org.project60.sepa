<?php
/*-------------------------------------------------------+
| Project 60 - SEPA direct debit                         |
| Copyright (C) 2017-2018 SYSTOPIA                       |
| Author: B. Endres (endres -at- systopia.de)            |
| http://www.systopia.de/                                |
+--------------------------------------------------------+
| This program is released as free software under the    |
| Affero GPL license. You can redistribute it and/or     |
| modify it under the terms of this license which you    |
| can read by viewing the included agpl.txt or online    |
| at www.gnu.org/licenses/agpl.html. Removal of this     |
| copyright header is strictly prohibited without        |
| written permission from the original author(s).        |
+--------------------------------------------------------*/

define('SDD_CLOSE_RUNNER_BATCH_SIZE', 250);


/**
 * Queue Item for updating a sepa group
 */
class CRM_Sepa_Logic_Queue_Close {

  /**
   * Create a task for the queue
   *
   * @param $mode string
   * @param $params array
   * @return CRM_Queue_Task
   */
  private static function createTask($mode, $params = []) {
    $task = new CRM_Queue_Task(
      ['CRM_Sepa_Logic_Queue_Close', 'run'],
      [$mode, $params],
      self::taskTitle($mode, $params)
    );

    $task->runAs = [
      'contactId' => CRM_Core_Session::getLoggedInContactID(),
      'domainId'  => 1,
    ];

    return $task;
  }

  /**
   * Use Civi::queue to close a SDD group
   */
  public static function launchCloseRunner($txgroup_ids, $target_group_status, $target_contribution_status) {
    // Create a queue
    $queue = Civi::queue('sdd_close', [
      'error'  => 'abort',
      'reset'  => FALSE,
      'runner' => 'task',
      'type'   => 'Sql',
    ]);

    // Fetch the groups
    $txgroup_query = civicrm_api3('SepaTransactionGroup', 'get', [
      'id'           => [ 'IN' => $txgroup_ids ],
      'option.limit' => 0,
    ]);

    $group_status_id_busy = (int) CRM_Core_PseudoConstant::getKey(
      'CRM_Batch_BAO_Batch',
      'status_id',
      'Data Entry'
    );

    foreach ($txgroup_query['values'] as $txgroup) {
      $txgroup_id = $txgroup['id'];

      // Set group status to busy
      $queue->createItem(self::createTask('set_group_status', [
        'target_status_id' => $group_status_id_busy,
        'txgroup'          => $txgroup,
      ]));

      // Count the contributions and create an appropriate amount of items
      $contribution_count = CRM_Core_DAO::singleValueQuery("
        SELECT COUNT(contribution_id) FROM civicrm_sdd_contribution_txgroup
        WHERE txgroup_id = $txgroup_id
      ");

      // Security margin
      $contribution_count += SDD_CLOSE_RUNNER_BATCH_SIZE;

      for ($offset = 0; $offset <= $contribution_count; $offset += SDD_CLOSE_RUNNER_BATCH_SIZE) {
        $queue->createItem(self::createTask('update_contribution', [
          'counter'          => $offset,
          'target_status_id' => $target_contribution_status,
          'txgroup'          => $txgroup,
        ]));
      }

      // Render XML and mark the group
      $queue->createItem(self::createTask('create_xml', [
        'target_status_id' => $target_group_status,
        'txgroup'          => $txgroup,
      ]));

      $queue->createItem(self::createTask('set_group_status', [
        'target_status_id' => $target_group_status,
        'txgroup'          => $txgroup,
      ]));

      $bgqueue_enabled = (bool) Civi::settings()->get('enableBackgroundQueue');

      if (!$bgqueue_enabled) {
        $runner_title = ts('Closing SDD Group(s) [%1]', [
          1        => implode(',', $txgroup_ids),
          'domain' => 'org.project60.sepa',
        ]);

        $runner = new CRM_Queue_Runner([
          'errorMode' => CRM_Queue_Runner::ERROR_ABORT,
          'onEndUrl'  => CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=closed'),
          'queue'     => $queue,
          'title'     => $runner_title,
        ]);

        $runner->runAllViaWeb();
      }
    }
  }

  /**
   * Execute a task
   *
   * @param $_ctx CRM_Queue_TaskContext
   * @param $mode string
   * @param $params array
   * @return bool
   */
  public static function run($_ctx, $mode, $params) {
    $_ctx->queue->setStatus('active');

    $target_status_id = $params['target_status_id'] ?? NULL;
    $txgroup = $params['txgroup'] ?? NULL;

    switch ($mode) {
      case 'update_contribution':
        // This one needs a lock
        $exception = NULL;
        $lock = CRM_Sepa_Logic_Settings::getLock();

        if (empty($lock)) {
          throw new Exception("Batching in progress. Please try again later.");
        }

        try {
          self::updateContributions($txgroup, $target_status_id);
        } catch (Exception $e) {
          // Store and throw later
          $exception = $e;
        }

        $lock->release();

        if ($exception) throw $exception;

        break;

      case 'create_xml':
        // Create the SEPA file
        civicrm_api3('SepaAlternativeBatching', 'createxml', [
          'txgroup_id' => $txgroup['id'],
        ]);

        break;

      case 'set_group_status':
        // Simply change the status
        civicrm_api3('SepaTransactionGroup', 'create', [
          'id'        => $txgroup['id'],
          'status_id' => $target_status_id,
        ]);

        break;

      default:
        return FALSE;
    }

    return TRUE;
  }

  /**
   * Will select the next batch of up to SDD_CLOSE_RUNNER_BATCH_SIZE
   * contributions and update their status
   *
   * @param $txgroup array
   * @param $target_status_id int
   * @return void
   */
  protected static function updateContributions($txgroup, $target_status_id) {
    $status_pending = (int) CRM_Core_PseudoConstant::getKey(
      'CRM_Contribute_BAO_Contribution',
      'contribution_status_id',
      'Pending'
    );

    $status_in_progress = CRM_Sepa_Logic_Settings::contributionInProgressStatusId();

    // Get eligible contributions (slightly different queries for OOFF/RCUR)
    if ($txgroup['type'] == 'OOFF') {
      $query = CRM_Core_DAO::executeQuery("
        SELECT
          civicrm_sdd_mandate.id                      AS mandate_id,
          civicrm_sdd_mandate.status                  AS mandate_status,
          civicrm_contribution.id                     AS contribution_id,
          civicrm_contribution.contribution_status_id AS contribution_status_id
        FROM civicrm_sdd_contribution_txgroup
        LEFT JOIN civicrm_contribution
          ON civicrm_contribution.id = civicrm_sdd_contribution_txgroup.contribution_id
        LEFT JOIN civicrm_sdd_mandate
          ON civicrm_sdd_mandate.entity_id = civicrm_contribution.id
          AND civicrm_sdd_mandate.entity_table = 'civicrm_contribution'
        WHERE civicrm_sdd_contribution_txgroup.txgroup_id = %1
          AND (
            civicrm_contribution.contribution_status_id = %2
            OR civicrm_contribution.contribution_status_id = %3
          )
          AND (civicrm_contribution.contribution_status_id <> %4)
        LIMIT %5
      ", [
        1 => [$txgroup['id'], 'Integer'],
        2 => [$status_pending, 'Integer'],
        3 => [$status_in_progress, 'Integer'],
        4 => [$target_status_id, 'Integer'],
        5 => [SDD_CLOSE_RUNNER_BATCH_SIZE, 'Integer'],
      ]);
    } elseif (
      $txgroup['type'] == 'RCUR'
      || $txgroup['type'] == 'FRST'
      || $txgroup['type'] == 'RTRY'
    ) {
      $query = CRM_Core_DAO::executeQuery("
        SELECT
          civicrm_sdd_mandate.id                      AS mandate_id,
          civicrm_sdd_mandate.status                  AS mandate_status,
          civicrm_contribution.id                     AS contribution_id,
          civicrm_contribution.contribution_status_id AS contribution_status_id
        FROM civicrm_sdd_contribution_txgroup
        LEFT JOIN civicrm_contribution
          ON civicrm_contribution.id = civicrm_sdd_contribution_txgroup.contribution_id
        LEFT JOIN civicrm_contribution_recur
          ON civicrm_contribution_recur.id = civicrm_contribution.contribution_recur_id
        LEFT JOIN civicrm_sdd_mandate
          ON civicrm_sdd_mandate.entity_id = civicrm_contribution_recur.id
          AND civicrm_sdd_mandate.entity_table = 'civicrm_contribution_recur'
        WHERE civicrm_sdd_contribution_txgroup.txgroup_id = %1
          AND (
            civicrm_contribution.contribution_status_id = %2
            OR civicrm_contribution.contribution_status_id = %3
          )
          AND (civicrm_contribution.contribution_status_id <> %4)
        LIMIT %5
      ", [
        1 => [$txgroup['id'], 'Integer'],
        2 => [$status_pending, 'Integer'],
        3 => [$status_in_progress, 'Integer'],
        4 => [$target_status_id, 'Integer'],
        5 => [SDD_CLOSE_RUNNER_BATCH_SIZE, 'Integer'],
      ]);
    } else {
      throw new Exception("Illegal group type '{$txgroup['type']}'", 1);
    }

    // Collect the data
    $contributions = [];

    while ($query->fetch()) {
      $contributions[$query->contribution_id] = [
        'contribution_status_id' => $query->contribution_status_id,
        'id'                     => $query->contribution_id,
        'mandate_id'             => $query->mandate_id,
        'mandate_status'         => $query->mandate_status,
      ];
    }

    // If there's nothing to do, stop right here
    if (empty($contributions)) return;

    // Update the contribution status
    self::updateContributionStatus($contributions);

    // Update the mandate status
    if ($txgroup['type'] == 'OOFF') {
      self::updateMandateStatus($contributions, 'SENT', 'OOFF');
    } elseif ($txgroup['type'] == 'FRST') {
      // TODO: GET $collection_date
      self::updateMandateStatus($contributions, 'RCUR', 'FRST');
    }

    // Also update next collection date
    if (
      $txgroup['type'] == 'FRST'
      || $txgroup['type'] == 'RCUR'
      || $txgroup['type'] == 'RTRY'
    ) {
      CRM_Sepa_Logic_NextCollectionDate::advanceNextCollectionDate(
        NULL,
        array_keys($contributions)
      );
    }
  }


  /**
   * Update the status of all the given contributions
   *
   * @param $contributions array
   * @param $new_status string
   * @param $for_old_status string
   * @return void
   */
  protected static function updateMandateStatus($contributions, $new_status, $for_old_status) {
    foreach ($contributions as $contribution) {
      if ($contribution['mandate_status'] == $for_old_status) {
        // The mandate has the required status
        $update = [
          'id'     => $contribution['mandate_id'],
          'status' => $new_status,
        ];

        if ($new_status=='RCUR' && $contribution['mandate_status'] == 'FRST') {
          // In this case we also want to set the contribution as first
          $update['first_contribution_id'] = $contribution['id'];
        }

        civicrm_api3('SepaMandate', 'create', $update);
      }
    }
  }

  /**
   * Update the status of all given contributions to $target_status_id
   *
   * @param $contributions array
   * @param $txgroup array
   * @param $target_status_id int
   * @return void
   */
  protected static function updateContributionStatus($contributions, $txgroup, $target_status_id) {
    $contribution_id_list = implode(',', array_keys($contributions));
    $status_in_progress = CRM_Sepa_Logic_Settings::contributionInProgressStatusId();

    if (empty($contribution_id_list)) {
      // This would cause SQL errors
      return;
    }

    if ($target_status_id == $status_in_progress) {
      // This status cannot be set via the API -> use SQL
      CRM_Core_DAO::executeQuery("
        UPDATE civicrm_contribution SET contribution_status_id = $status_in_progress
        WHERE id IN ($contribution_id_list);
      ");
    } else {
      // This should be status 'Completed', but it doesn't really matter
      // Sanity checks:
      if (version_compare(CRM_Utils_System::version(), '4.7.0', '>=')) {
        // Make sure they're all in status 'In Progress' to avoid SEPA-514
        CRM_Core_DAO::executeQuery("
          UPDATE civicrm_contribution SET contribution_status_id = $status_in_progress
          WHERE id IN ($contribution_id_list);
        ");
      }

      // Set them all to the new status
      foreach ($contributions as $contribution) {
        $receive_date = date('YmdHis', strtotime($txgroup['collection_date']));

        civicrm_api3('Contribution', 'create', [
          'contribution_status_id' => $target_status_id,
          'id'                     => $contribution['id'],
          'receive_date'           => $receive_date,
        ]);
      }
    }
  }

  /**
   * Render a title for a task
   *
   * @param $mode string
   * @param $params array
   * @return string
   */
  private static function taskTitle($mode, $params = []) {
    $counter = $params['counter'] ?? NULL;
    $target_status_id = $params['target_status_id'] ?? NULL;
    $txgroup_ref = $params['txgroup']['reference'] ?? NULL;

    switch ($mode) {
      case 'update_contribution':
        return ts("Updating contributions in group '%1'... (%2)", [
          1        => $txgroup_ref,
          2        => $counter,
          'domain' => 'org.project60.sepa',
        ]);

      case 'set_group_status':
        return ts("Updating status of group '%1' (Status ID: %2)", [
          1        => $txgroup_ref,
          2        => $target_status_id,
          'domain' => 'org.project60.sepa',
        ]);

      case 'create_xml':
        return ts("Compiling XML for group '%1'", [
          1        => $txgroup_ref,
          'domain' => 'org.project60.sepa',
        ]);

      default:
        return ts('Unknown', [ 'domain' => 'org.project60.sepa' ]);
    }
  }

}


