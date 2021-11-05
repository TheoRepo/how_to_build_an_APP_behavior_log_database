#!/bin/bash -e

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/2_create_strategy_table.sh qianyu
bash ${baseDirForScriptSelf}/3_insert_data.sh qianyu
bash ${baseDirForScriptSelf}/4_input_new_data.sh qianyu