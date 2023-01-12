from typing import Dict

CMD_CUSTOMER_COLUMN_REPLACE = {
    'Id': 'id',
    'CustomerNumber': 'customer_number',
    'CampanyNameOne': 'company_name_one',
    'CampanyNameTwo': 'company_name_two',
    'ShortCampanyName': 'short_company_name',
    'TradeNameOne': 'trade_name_one',
    'TradeNameTwo': 'trade_name_two',
    'Street': 'street',
    'HouseNumber': 'house_number',
    'HouseNumberComplement': 'house_number_complement',
    'ShortAddress': 'short_address',
    'ReferencePointOne': 'reference_point_one',
    'ReferencePointTwo': 'reference_point_two',
    'District': 'district',
    'City': 'city',
    'PostalCode': 'postal_code',
    'Country_Id': 'country_id',
    'Region': 'region',
    'CNPJ_CPFNumber': 'cnpj_cpf_number',
    'StateTaxNumber': 'state_tax_number',
    'MunicipalTaxNumber': 'municipal_tax_number',
    'TypeOfEstablishment_Id': 'type_of_establishment_id',
    'RouteSales': 'route_sales',
    'BusnOperationSiteCode': 'busn_operation_site_code',
    'BusinessPartnerCategory_Id': 'business_partner_category_id',
    'BusinessPartnerType_Id': 'business_partner_type_id',
    'BlockingReason_Id': 'blocking_reason_id',
    'CreateOn': 'create_on',
    'ModDate': 'mod_date',
    'CreatedByUserId': 'created_by_user_id',
    'CreatedByUser': 'created_by_user',
    'IsCPF': 'is_cpf',
    'Status': 'status',
    'PrivacyPolicy': 'privacy_policy',
    'AttendanceFrequency_Id': 'attendance_frequency_id',
    'Weekdays': 'weekdays',
    'ServiceSequence': 'service_sequence',
    'DayClosed': 'day_closed',
    'OpeningTime': 'opening_time',
    'ClosingTime': 'closing_time',
    'Anonymized': 'anonymized',
    'PreviousCustomerNumber': 'previous_customer_number',
    'ProofOfAddressEstablishment_Id': 'proof_of_address_establishment_id',
    'DocumentTypes': 'document_types'
}

CMD_PHONES_COLUMN_REPLACE = {
    'Id': 'id',
    'CustomerID': 'customer_id',
    'TelephoneNumber': 'telephone_number',
    'IsMobilePhone': 'is_mobile_phone',
    'WhatsApp': 'whats_app',
}

CMD_EMAILS_COLUMN_REPLACE = {
    'Id': 'id',
    'CustomerID': 'customer_id',
    'eMail': 'email',
    'Nfrel': 'nfrel',
    'Audit': 'audit',
    'EmailConfirmed': 'email_confirmed',
    'SecurityToken': 'security_token',
}

CMD_CONTACTS_COLUMN_REPLACE = {
    'Id': 'id',
    'CustomerID': 'customer_id',
    'FirstName': 'first_name',
    'LastName': 'last_name',
    'Function': 'function',
}

CMD_COUNTRIES_COLUMN_REPLACE = {
    'Id': 'id',
    'Code': 'code',
    'Description': 'description',
}

CMD_PARTNERS_COLUMN_REPLACE = {
    'Id': 'id',
    'CustomerID': 'customer_id',
    'Name': 'name',
    'CPF': 'cpf',
    'CapitalPencentage': 'capital_percentage',
}

CMD_BUSINESS_PARTNER_CATEGORY_COLUMN_REPLACE = {
    'Id': 'id',
    'Name': 'name',
    'Code': 'code',
}

CMD_BUSINESS_PARTNER_TYPE_COLUMN_REPLACE = {
    'Id': 'id',
    'Name': 'name',
    'Code': 'code',
}

CMD_TYPE_OF_ESTABLISHMENT_COLUMN_REPLACE = {
    'Id': 'id',
    'Code': 'code',
    'Name': 'name',
}

CMD_ATTENDANCE_FREQUENCY_COLUMN_REPLACE = {
    'Id': 'id',
    'Code': 'code',
    'Description': 'description',
    'VisibleOn': 'visible_on',
}

CMD_PROOF_OF_ADDRESS_ESTABLISHMENTS_COLUMN_REPLACE = {
    'Id': 'id',
    'Code': 'code',
    'Description': 'description',
}

CMD_BLOCKING_REASON_COLUMN_REPLACE = {
    'Id': 'id',
    'Code': 'code',
    'Description': 'description',
    'VisibleOn': 'visible_on'
}


class TableProcess:
    def __init__(self,
                 table_name: str,
                 key: str,
                 _type: str,
                 compare_key: str,
                 column_dict: Dict[str, str]):

        self.table_name = table_name
        self.key = key
        self.format_type = _type
        self.column_dict = column_dict
        self.compare_key = compare_key


PHONE_TABLE_PROCESS = TableProcess('cmd_phones_stage',
                                   'Phones',
                                   'flatten',
                                   'customer_id',
                                   CMD_PHONES_COLUMN_REPLACE)

EMAILS_TABLE_PROCESS = TableProcess('cmd_emails_stage',
                                    'Emails',
                                    'flatten',
                                    'customer_id',
                                    CMD_EMAILS_COLUMN_REPLACE)

CONTACTS_TABLE_PROCESS = TableProcess('cmd_contacts_stage',
                                      'Contacts',
                                      'flatten',
                                      'customer_id',
                                      CMD_CONTACTS_COLUMN_REPLACE)

PARTNERS_TABLE_PROCESS = TableProcess('cmd_partners_stage',
                                      'Partners',
                                      'flatten',
                                      'customer_id',
                                      CMD_PARTNERS_COLUMN_REPLACE)

COUNTRIES_TABLE_PROCESS = TableProcess('cmd_countries_stage',
                                       'Country',
                                       'remove_nulls',
                                       'id',
                                       CMD_COUNTRIES_COLUMN_REPLACE)

PARTNER_CATEGORY_TABLE_PROCESS = TableProcess('cmd_business_partner_category_stage',
                                              'BusinessPartnerCategory',
                                              'remove_nulls',
                                              'id',
                                              CMD_BUSINESS_PARTNER_CATEGORY_COLUMN_REPLACE)

PARTNER_TYPE_TABLE_PROCESS = TableProcess('cmd_business_partner_type_stage',
                                          'BusinessPartnerType',
                                          'remove_nulls',
                                          'id',
                                          CMD_BUSINESS_PARTNER_TYPE_COLUMN_REPLACE)

TYPE_OF_ESTABLISHMENT_TABLE_PROCESS = TableProcess('cmd_type_of_establishment_stage',
                                                   'TypeOfEstablishment',
                                                   'remove_nulls',
                                                   'id',
                                                   CMD_TYPE_OF_ESTABLISHMENT_COLUMN_REPLACE)

ATTENDANCE_FREQUENCY_TABLE_PROCESS = TableProcess('cmd_attendance_frequency_stage',
                                                  'AttendanceFrequency',
                                                  'remove_nulls',
                                                  'id',
                                                  CMD_ATTENDANCE_FREQUENCY_COLUMN_REPLACE)

ADDRESS_ESTABLISHMENT_TABLE_PROCESS = TableProcess('cmd_proof_of_address_establishments_stage',
                                                   'ProofOfAddressEstablishment',
                                                   'remove_nulls',
                                                   'id',
                                                   CMD_PROOF_OF_ADDRESS_ESTABLISHMENTS_COLUMN_REPLACE)

BLOCKING_REASON_TABLE_PROCESS = TableProcess('cmd_proof_blocking_reason',
                                             'BlockingReason',
                                             'remove_nulls',
                                             'id',
                                             CMD_BLOCKING_REASON_COLUMN_REPLACE)
