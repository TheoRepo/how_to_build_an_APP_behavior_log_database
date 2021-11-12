#!/bin/bash

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh
bash ${baseDirForScriptSelf}/1_all_mobile_id.sh
bash ${baseDirForScriptSelf}/2_app_life.sh
bash ${baseDirForScriptSelf}/3_app_no_life.sh