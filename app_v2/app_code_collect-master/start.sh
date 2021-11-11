#!/bin/bash -e

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/1_extract_011.sh 2021-01-02 011 ga '' preprocess.ds_txt_final profile.dws_app_regexp_20211030
