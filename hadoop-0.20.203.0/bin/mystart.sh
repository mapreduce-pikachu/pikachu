rm .././logs/*
rm -rf .././logs/userlogs/*
./start-dfs.sh
./start-mapred.sh
echo "sleeping"
sleep 40
