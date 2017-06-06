# Logic numbering of screens and tests

**Description**: The following explains the logic used to identify screens and tests config files.  
All screen and tests have an identifier number.  

**Cases**:  
* **`id_project`**: Project identifier. (For gplatform-global is `1`)

* **`id_fileentity`**: Compound by **_id_project_** and **_interface identifier_**.

        Example: [ gplatform_global identifier is 1 ] UNION [ customer identifier is 01 ] = [ id_fileentity is 101 ]
* **`id_screen`**: Compound by **_id_project_**, **_id_fileentity_**, **_number_** from from 01 to 99

        Example: [ gplatform_global identifier is 1 ] UNION [ customer identifier is 01 ] UNION [ any screen identifier as 04 ] = [ id_screen is 10104 ]
* **`id_test`**: Compound by **_id_project_**, **_id_fileentity_**, **_number_** from from 01 to 99

        Example: [ gplatform_global identifier is 1 ] UNION [ customer identifier is 01 ] UNION [ any test identifier as 04 ] = [ id_test is 10104 ]  

**Numbering of Interfaces**:  
* `01: customer`
* `02: daily_traffic`
* `03: dim_f_tariff_plan`
* `04: dim_f_voice_type`
* `05: dim_group_sva`
* `06: dim_m_services`
* `07: dim_m_tariff_plan`
* `08: dim_postal`
* `09: f_access`
* `10: f_lines`
* `11: f_tariff_plan`
* `12: handset_upgrade`
* `13: imei_sales`
* `14: invoice`
* `15: m_lines`
* `16: movements`
* `17: services_line`: This one is deprecated. Use `m_line_services` instead.
* `18: traffic_data`
* `19: traffic_sms`
* `20: traffic_voice`
* `21: dim_m_billing_cycle`
* `22: dim_m_campaign`
* `23: dim_m_movement`
* `24: dim_m_operators`
* `25: m_line_services`
* `26: multisim`
