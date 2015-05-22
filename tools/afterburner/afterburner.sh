#!/bin/bash

. helper_afterburner.sh

check_is_user_root
check_required_apps

count_eth_queues
get_cpu_socket_cores

NCS=1 # jerry-rig NCS for non cpu-socket aware community edition

if [ $NETH -gt 1 -a $NUM_TOT_ETH_QUEUES -gt 1 ]; then
  echo "Number of Ethernet interfaces detected: ($NETH)"
  echo "Apply NIQ queue IRQ smp_affinity to ALL ethernet interfaces: [y/n]"
  read ans
  if [ "$ans" != "Y" -a "$ans" != "y" ]; then
    echo "Skipping NIQ queue IRQ smp_affinity optimizations"
    exit 0;
  fi
fi
I=0; while [ $I -lt $NETH ]; do
  CL_ETH[$I]=${ETH[$I]}
  CL_NUM_QUEUES_PER_ETH[$I]=${NUM_QUEUES_PER_ETH[$I]}
  CL_IRQS_PER_ETH[$I]=${IRQS_PER_ETH[$I]}
  I=$[${I}+1];
done

I=0; while [ $I -lt $NETH ]; do
  J=0; for irq in ${CL_IRQS_PER_ETH[$I]}; do
    IRQ[$J]=$irq J=$[${J}+1];
  done
  J=0; while [ $J -lt $NUM_TOT_CPU_CORES -a $J -lt ${CL_NUM_QUEUES_PER_ETH[$I]} ]; do
    echo "Configuring: core: $J ETH: ${CL_ETH[$I]} " \
         "IRQ: ${IRQ[$J]} AFFINITY: ${IRQ_AFFINITY_FOR_CORE[${J}]}"
    echo ${IRQ_AFFINITY_FOR_CORE[${J}]} > \
         /proc/irq/${IRQ[$J]}/smp_affinity
    J=$[${J}+1];
  done
  I=$[${I}+1];
done

# EDIT config file for Optimal Thread-Counts for in-memory loads
CONF="/etc/aerospike/aerospike.conf"

NCORES=$NUM_TOT_CPU_CORES
num_tsvc_q=$NCORES
num_tsvc=3
num_dm=$NCORES

sed -i 's/service-threads .*/service-threads '$num_dm'/' $CONF
sed -i 's/transaction-queues .*/transaction-queues '$num_tsvc_q'/' $CONF
sed -i 's/transaction-threads-per-queue .*/transaction-threads-per-queue '$num_tsvc'/' $CONF
sed -i 's/client-fd-max .* #/client-fd-max 15000 #/' $CONF

exit 0;
