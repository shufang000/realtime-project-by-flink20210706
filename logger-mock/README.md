# 工程简介

# 延伸阅读

yum -y install make zlib zlib-devel gcc-c++ libtool  openssl openssl-devel
tar -zxvf nginx-1.12.2.tar.gz

cd nginx-1.12.2/
./configure  --prefix=/opt/module/nginx

make && make install
cd /opt/module/nginx/

set cap_net_bind_service=+eip /opt/module/nginx/sbin/nginx