#!/bin/bash

# check if train_loss.csv exist
loss_file=data/ect_loss.csv
if [ -e "$loss_file" ]; then
    rm $loss_file
    echo "train/test,learning_rate,passes,average_loss" >>$loss_file
fi

# loop for training
for n_passes in `seq 1 20`
do
    # number of passes
    for p in `seq -4 2`
    do
        # learning rate
        l_rate=`echo "scale=2; 2^$p" | bc -l`

        # prompt
        echo " "
        echo "-------------------ect train------------------------"
        echo "-------------------round: $n_passes--------------------"
        echo "-------------------step: $l_rate--------------------"
        echo " "

        # train model
        vw_train="vw --ect 26 isolet_train.vw --cache_file cache_train --sgd -l $l_rate --passes $n_passes -f ect.model"

        # save terminal output to train_log.txt
        script -c "$vw_train" train_log.txt

        # extract average train loss
        avg_loss=`cat train_log.txt | grep "average loss = " | grep -o -E "([0-9]+.[0-9]+)|((\+|-)nan)"`
        echo "1,${l_rate},${n_passes},${avg_loss}" >>$loss_file

        # prompt
        echo " "
        echo "--------------------ect test------------------------"
        echo "--------------------round: $n_passes--------------------"
        echo "--------------------step: $l_rate---------------------"
        echo " "

        # test model
        vw_test="vw -t --cache_file cache_test -i ect.model isolet_test.vw"
        # save terminal output to test_log.txt
        script -c "$vw_test" test_log.txt
        # extract average test loss
        avg_loss=`cat test_log.txt | grep "average loss = " | grep -o -E "[0-9]+.[0-9]+"`
        echo "0,${l_rate},${n_passes},${avg_loss}" >>$loss_file
    done
done

rm test_log.txt train_log.txt 
