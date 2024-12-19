<?php

/**
 * DAOs provide an OOP-style facade for reading and writing database records.
 *
 * DAOs are a primary source for metadata in older versions of CiviCRM (<5.74)
 * and are required for some subsystems (such as APIv3).
 *
 * This stub provides compatibility. It is not intended to be modified in a
 * substantive way. Property annotations may be added, but are not required.
 * @property string $id 
 * @property string $reference 
 * @property string $type 
 * @property string $collection_date 
 * @property string $latest_submission_date 
 * @property string $created_date 
 * @property string $status_id 
 * @property string $sdd_creditor_id 
 * @property string $sdd_file_id 
 */
class CRM_Sepa_DAO_SEPATransactionGroup extends CRM_Sepa_DAO_Base {

  /**
   * Required by older versions of CiviCRM (<5.74).
   * @var string
   */
  public static $_tableName = 'civicrm_sdd_txgroup';

}
