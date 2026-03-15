#!/bin/bash
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=1000
#SBATCH --time=7-00:00:00
#SBATCH -e sync_logs/rclone.err
#SBATCH -o sync_logs/rclone.out
module load hurcs rclone

cd /sci/labs/dh_huji/liri.sokol/journals-keyword-searching

source_dir="pdfs"
dest_dir="G-l:"
log_path=sync_logs/rclone.log
rclone sync \
  --drive-root-folder-id=1iqqwrcwqRxoKyxNuZeVsVMubLMgt864H \
  --fast-list \
  --cache-rps 50 -v --tpslimit 20 --tpslimit-burst 20 \
  --checkers 32 --transfers 16 \
  "$source_dir" "$dest_dir" 2>&1 | tee $log_path

source_dir="search_results"
dest_dir="G-l:"
log_path=sync_logs/rclone.log
rclone sync \
  --drive-root-folder-id=1l6JMQIJ3TCovPncj_aBXbw-K5CILDxux \
  --fast-list \
  --cache-rps 50 -v --tpslimit 20 --tpslimit-burst 20 \
  --checkers 32 --transfers 16 \
  "$source_dir" "$dest_dir" 2>&1 | tee $log_path
