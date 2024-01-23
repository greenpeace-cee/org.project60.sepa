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
    $task = new CRM_Queue_Task(
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
   * Determine the count of mandates to be investigated
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
    $queue_sequential = Civi::queue('sdd_update_sequential', [
      'error'      => 'abort',
      'lease_time' => 60 * 60 * 24, // 24 hours
      'reset'      => TRUE,
      'runner'     => 'task',
      'type'       => 'Sql',
    ]);

    // Check whether background queues are enabled
    $bgqueue_enabled = (bool) Civi::settings()->get('enableBackgroundQueue');

    // Close outdated groups
    $queue_sequential->createItem(self::createTask('PREPARE'));
    $queue_sequential->createItem(self::createTask('CLOSE'));

    // Then iterate through all creditors
    $creditors = civicrm_api3('SepaCreditor', 'get', [ 'option.limit' => 0 ]);

    foreach ($creditors['values'] as $creditor) {
      $sdd_modes = ($mode == 'RCUR') ? ['FRST', 'RCUR'] : ['OOFF'];

      foreach ($sdd_modes as $sdd_mode) {
        // Safety margin
        $count = self::getMandateCount($creditor['id'], $sdd_mode) + SDD_UPDATE_RUNNER_BATCH_SIZE;

        if ($bgqueue_enabled) {
          $queue_sequential->createItem(self::createTask('UPDATE_ALL', [
            'count'       => $count,
            'creditor_id' => $creditor['id'],
            'mode'        => $sdd_mode,
          ]));
        } else {
          for ($offset=0; $offset < $count; $offset += SDD_UPDATE_RUNNER_BATCH_SIZE) {
            $queue_sequential->createItem(self::createTask('UPDATE', [
              'creditor_id' => $creditor['id'],
              'mode'        => $sdd_mode,
              'offset'      => $offset,
            ]));
          }
        }

        $queue_sequential->createItem(self::createTask('CLEANUP', [ 'mode' => $sdd_mode ]));
      }
    }

    $queue_sequential->createItem(self::createTask('FINISH'));

    if (!$bgqueue_enabled) {
      $runner_title = ts('Updating %1 SEPA Groups', [
        1        => $mode,
        'domain' => 'org.project60.sepa',
      ]);

      $runner = new CRM_Queue_Runner([
        'errorMode' => CRM_Queue_Runner::ERROR_ABORT,
        'onEndUrl'  => CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active'),
        'queue'     => $queue_sequential,
        'title'     => $runner_title,
      ]);

      $runner->runAllViaWeb();
    }
  }

  /**
   * Execute a task
   *
   * @param $_ctx CRM_Queue_TaskContext
   * @param $cmd string
   * @param $params array
   * @return bool
   */
  public static function run($_ctx, $cmd, $params) {
    $_ctx->queue->setStatus('active');

    $count = $params['count'] ?? 0;
    $creditor_id = $params['creditor_id'] ?? NULL;
    $limit = SDD_UPDATE_RUNNER_BATCH_SIZE;
    $mode = $params['mode'] ?? NULL;
    $offset = $params['offset'] ?? NULL;

    switch ($cmd) {
      case 'PREPARE': {
        // nothing to do
        break;
      }

      case 'CLOSE': {
        CRM_Sepa_Logic_Batching::closeEnded();

        CRM_Sepa_Logic_Settings::renewAsyncLock(
          'sdd_async_update_lock',
          SDD_UPDATE_RUNNER_BATCH_LOCK_TIMOUT
        );

        break;
      }

      case 'UPDATE': {
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
      }

      case 'UPDATE_ALL': {
        $queue_parallel = Civi::queue('sdd_update_parallel', [
          'error'  => 'abort',
          'reset'  => TRUE,
          'runner' => 'task',
          'type'   => 'SqlParallel',
        ]);

        for ($offset=0; $offset < $count; $offset += SDD_UPDATE_RUNNER_BATCH_SIZE) {
          $queue_parallel->createItem(self::createTask('UPDATE', [
            'creditor_id' => $creditor_id,
            'mode'        => $mode,
            'offset'      => $offset,
          ]));
        }

        do {
          sleep(5);

          $task_count = (int) CRM_Core_DAO::singleValueQuery("
            SELECT count(*) FROM civicrm_queue_item WHERE queue_name = 'sdd_update_parallel'
          ");
        } while ($task_count > 0);

        break;
      }

      case 'CLEANUP': {
        CRM_Sepa_Logic_Group::cleanup($mode);
        break;
      }

      case 'FINISH': {
        CRM_Sepa_Logic_Settings::releaseAsyncLock('sdd_async_update_lock');
        break;
      }

      default: {
        return FALSE;
      }
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
    $creditor_id = $params['creditor_id'] ?? NULL;
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
          2 => $offset + SDD_UPDATE_RUNNER_BATCH_SIZE,
          'domain' => 'org.project60.sepa',
        ]);

      case 'UPDATE_ALL':
        return ts("Process all $mode mandates in parallel (Creditor ID: %1)", [
          1 => $creditor_id,
          'domain' => 'org.project60.sepa',
        ]);

      default:
        return ts('Unknown', [ 'domain' => 'org.project60.sepa' ]);
    }
  }

}


