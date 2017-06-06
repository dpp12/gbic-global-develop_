set INTERFACE=DAILY_TRAFFIC

!echo -e                                                                                                                           \n
SCRIPT NOT IMPLEMENTED                                                                                                             \n
Executing mock script for:                                                                                                         \n\t
                                                                                                                                   \n\t
    load_qa_${hiveconf:INTERFACE}.hql                                                                                              \n\t
                                                                                                                                   \n\t
    - reading file from: /user/gplatform/inbox/${hivevar:op3m}/MSv${hivevar:version}/${hiveconf:INTERFACE}/month=${hivevar:month}/ \n\t
    - using (gbic_op_id='${hivevar:op}', month='${hivevar:month}') for partitioning                                                \n\t
