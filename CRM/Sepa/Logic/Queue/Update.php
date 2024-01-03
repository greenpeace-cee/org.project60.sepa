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

define('SDD_UPDATE_RUNNER_BATCH_SIZE', 250);
define('SDD_UPDATE_RUNNER_BATCH_LOCK_TIMOUT', 600);


/**
 * Queue Item for updating a sepa group
 */
class CRM_Sepa_Logic_Queue_Update {

  /**
   * Create a task for the queue
   *
   * @param $cmd string
   * @param $params array
   * @return CRM_Queue_Task
   */
  private static function createTask($cmd, $params = []) {
    $task = CRM_Queue_Task(
      ['CRM_Sepa_Logic_Queue_Update', 'run'],
      [$cmd, $params],
      self::taskTitle($cmd, $params)
    );

    $task->runAs = [
      'contactId' => CRM_Core_Session::getLoggedInContactID(),
      'domainId'  => 1,
    ];

    return $task;
  }

  /**
   * determine the count of mandates to be investigated
   */
  protected static function getMandateCount($creditor_id, $sdd_mode) {
    if ($sdd_mode == 'OOFF') {
      $horizon = (int) CRM_Sepa_Logic_Settings::getSetting('batching.OOFF.horizon', $creditor_id);
      $date_limit = date('Y-m-d', strtotime("+$horizon days"));
      return CRM_Core_DAO::singleValueQuery("
        SELECT COUNT(mandate.id)
        FROM civicrm_sdd_mandate AS mandate
        INNER JOIN civicrm_contribution AS contribution  ON mandate.entity_id = contribution.id
        WHERE contribution.receive_date <= DATE('$date_limit')
          AND mandate.type = 'OOFF'
          AND mandate.status = 'OOFF'
          AND mandate.creditor_id = $creditor_id;");
    } else {
      return CRM_Core_DAO::singleValueQuery("
        SELECT
          COUNT(mandate.id)
        FROM civicrm_sdd_mandate AS mandate
        WHERE mandate.type = 'RCUR'
          AND mandate.status = '$sdd_mode'
          AND mandate.creditor_id = $creditor_id;");
    }
  }

  /**
   * Use Civi::queue to do the SDD group update
   *
   * @param $mode string
   * @return void
   */
  public static function launchUpdateRunner($mode) {
    $sdd_async_update_lock = CRM_Sepa_Logic_Settings::acquireAsyncLock(
      'sdd_async_update_lock',
      SDD_UPDATE_RUNNER_BATCH_LOCK_TIMOUT
    );

    if (!$sdd_async_update_lock) {
      CRM_Core_Session::setStatus(
        ts('Cannot run update, another update is in progress!', ['domain' => 'org.project60.sepa']),
        ts('Error'),
        'error'
      );

      $redirect_url = CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active');
      CRM_Utils_System::redirect($redirect_url);

      // Shouldn't be necessary
      return;
    }

    // Create a queue
    $queue = Civi::queue('sdd_update', [
      'error'  => 'delete',
      'reset'  => FALSE,
      'runner' => 'task',
      'type'   => 'SqlParallel',
    ]);

    // Close outdated groups
    $queue->createItem(self::createTask('PREPARE'));
    $queue->createItem(self::createTask('CLOSE'));

    // Then iterate through all creditors
    $creditors = civicrm_api3('SepaCreditor', 'get', [ 'option.limit' => 0 ]);

    foreach ($creditors['values'] as $creditor) {
      $sdd_modes = ($mode == 'RCUR') ? ['FRST', 'RCUR'] : ['OOFF'];

      foreach ($sdd_modes as $sdd_mode) {
        // Safety margin
        $count = self::getMandateCount($creditor['id'], $sdd_mode) + SDD_UPDATE_RUNNER_BATCH_SIZE;

        for ($offset=0; $offset < $count; $offset += SDD_UPDATE_RUNNER_BATCH_SIZE) {
          $queue->createItem(self::createTask('UPDATE', [
            'creditor_id' => $creditor['id'],
            'limit'       => SDD_UPDATE_RUNNER_BATCH_SIZE,
            'mode'        => $sdd_mode,
            'offset'      => $offset,
          ]));
        }

        $queue->createItem(self::createTask('CLEANUP', [ 'mode' => $sdd_mode ]));
      }
    }

    $queue->createItem(self::createTask('FINISH'));
  }

  /**
   * Execute a task
   * @param $_ctx CRM_Queue_TaskContext
   * @param $cmd string
   * @param $params array
   * @return bool
   */
  public static function run($_ctx, $cmd, $params) {
    $creditor_id = $params['creditor_id'] ?? NULL;
    $limit = $params['limit'] ?? NULL;
    $mode = $params['mode'] ?? NULL;
    $offset = $params['offset'] ?? NULL;

    switch ($cmd) {
      case 'PREPARE':
        // nothing to do
        break;

      case 'CLOSE':
        CRM_Sepa_Logic_Batching::closeEnded();

        CRM_Sepa_Logic_Settings::renewAsyncLock(
          'sdd_async_update_lock',
          SDD_UPDATE_RUNNER_BATCH_LOCK_TIMOUT
        );

        break;

      case 'UPDATE':
        if ($mode == 'OOFF') {
          CRM_Sepa_Logic_Batching::updateOOFF($creditor_id, 'now', $offset, $limit);
        } else {
          CRM_Sepa_Logic_Batching::updateRCUR($creditor_id, $mode, 'now', $offset, $limit);
        }

        CRM_Sepa_Logic_Settings::renewAsyncLock(
          'sdd_async_update_lock',
          SDD_UPDATE_RUNNER_BATCH_LOCK_TIMOUT
        );

        break;

      case 'CLEANUP':
        CRM_Sepa_Logic_Group::cleanup($mode);
        break;

      case 'FINISH':
        CRM_Sepa_Logic_Settings::releaseAsyncLock('sdd_async_update_lock');
        break;

      default:
        return FALSE;
    }

    return TRUE;
  }

  /**
   * Render a title for a task
   *
   * @param $cmd string
   * @param $params array
   * @return string
   */
  private static function taskTitle($cmd, $params) {
    $limit = $params['limit'] ?? 0;
    $mode = $params['mode'] ?? '[unknown]';
    $offset = $params['offset'] ?? 0;

    switch ($cmd) {
      case 'CLEANUP':
        return ts("Cleaning up $mode groups", [ 'domain' => 'org.project60.sepa' ]);

      case 'CLOSE':
        return ts('Cleaning up ended mandates', [ 'domain' => 'org.project60.sepa' ]);

      case 'FINISH':
        return ts('Lock released', [ 'domain' => 'org.project60.sepa' ]);

      case 'PREPARE':
        return ts('Preparing to clean up ended mandates', [ 'domain' => 'org.project60.sepa' ]);

      case 'UPDATE':
        return ts("Process $mode mandates (%1-%2)", [
          1 => $offset,
          2 => $offset + $limit,
          'domain' => 'org.project60.sepa',
        ]);

      default:
        return ts('Unknown', [ 'domain' => 'org.project60.sepa' ]);
    }
  }

}


