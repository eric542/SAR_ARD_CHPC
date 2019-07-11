
sleep 3600

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 1 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_1cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_1cpu/ --gpt_exec /home/leh026/snap/bin/gpt --scenes_per_job 2

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 2 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_2cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_2cpu/ --gpt_exec /home/leh026/snap/bin/gpt --scenes_per_job 3

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 4 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_4cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_4cpu/ --gpt_exec /home/leh026/snap/bin/gpt --scenes_per_job 4

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 8 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_8cpu2/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_8cpu2/ --gpt_exec /home/leh026/snap/bin/gpt

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 12 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_12cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_12cpu/ --gpt_exec /home/leh026/snap/bin/gpt

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 16 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_16cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_16cpu/ --gpt_exec /home/leh026/snap/bin/gpt

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 20 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_20cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_20cpu/ --gpt_exec /home/leh026/snap/bin/gpt

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 24 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_24cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_24cpu/ --gpt_exec /home/leh026/snap/bin/gpt

echo; echo
python3.6 CHPC_dualpol_proc_qsub.py --n_cpus 28 --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush3/leh026/SAR_DC/tests_3_dualpol/log_28cpu/ --base_data_dir /flush3/leh026/SAR_DC/S1_data_download/ --base_save_dir /flush3/leh026/SAR_DC/tests_3_dualpol/proc_data_28cpu/ --gpt_exec /home/leh026/snap/bin/gpt

