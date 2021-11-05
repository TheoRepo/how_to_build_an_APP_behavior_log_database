#!/bin/bash -e

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh qianyu ''
bash ${baseDirForScriptSelf}/1_collect_app_name.sh qianyu '' profile.dws_app_regexp_clean_20211030 ga.interface_app_strategy_20211030
bash ${baseDirForScriptSelf}/2_initial_index_table.sh qianyu ''
bash ${baseDirForScriptSelf}/3_update_status_yikaifa.sh qianyu '' profile.dws_app_regexp_clean_20211030
bash ${baseDirForScriptSelf}/4_update_status_paoshuyoushuju.sh qianyu '' profile.dws_app_all_20210530
bash ${baseDirForScriptSelf}/5_update_status_paoshuqiewushuju.sh qianyu ''

