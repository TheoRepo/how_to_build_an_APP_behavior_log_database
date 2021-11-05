#!/bin/bash -e

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh qianyu ''
bash ${baseDirForScriptSelf}/1_update_regexp_table_from_0530.sh qianyu ''
bash ${baseDirForScriptSelf}/2_nine_app_type.sh qianyu ''
bash ${baseDirForScriptSelf}/3_one_name_one_type.sh qianyu ''
bash ${baseDirForScriptSelf}/4_change_version_no.sh qianyu ''
bash ${baseDirForScriptSelf}/5_duplicate_mapping_app_name_on_different_version.sh qianyu ''
bash ${baseDirForScriptSelf}/6_one_mapping_app_name_one_version_no.sh qianyu ''
bash ${baseDirForScriptSelf}/8_unique_date.sh qianyu ''
bash ${baseDirForScriptSelf}/9_return_008_and_009.sh qianyu ''
bash ${baseDirForScriptSelf}/10_create_version_010_and_011_and_012.sh qianyu ''
bash ${baseDirForScriptSelf}/11_update_index.sh qianyu ''
bash ${baseDirForScriptSelf}/12_unify_null.sh qianyu ''