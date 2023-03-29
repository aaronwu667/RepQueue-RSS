for pid in $(ps -ef | grep "repl_store" | awk '{print $2}'); do kill -9 $pid; done;
for pid in $(ps -ef | grep "txn_manager" | awk '{print $2}'); do kill -9 $pid; done;
