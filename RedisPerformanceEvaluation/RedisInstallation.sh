echo "Starting REDIS Installation"
wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
cd redis-stable
make
sudo apt-get install redis-server
echo "Redis Installation Completed"
echo "Installing Ruby"
sudo apt-get install ruby
echo "Ruby Installation Completed"
sudo gem install redis
echo "All setup Completed for REDIS"