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
 * @property string $source 
 * @property string $entity_table 
 * @property string $entity_id 
 * @property string $date 
 * @property string $creditor_id 
 * @property string $contact_id 
 * @property string $account_holder 
 * @property string $iban 
 * @property string $bic 
 * @property string $type 
 * @property string $status 
 * @property string $creation_date 
 * @property string $first_contribution_id 
 * @property string $validation_date 
 */
class CRM_Sepa_DAO_SEPAMandate extends CRM_Sepa_DAO_Base {

  /**
   * Required by older versions of CiviCRM (<5.74).
   * @var string
   */
  public static $_tableName = 'civicrm_sdd_mandate';

}
