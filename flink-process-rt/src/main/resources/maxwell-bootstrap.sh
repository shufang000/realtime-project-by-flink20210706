#!/bin/bash
#利用maxwell-bootstrap进行初始化,同步全量数据

#[shufang@shufang101 maxwell-1.25.3]$ bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table sku_info --client_id maxwell_1 --config  config.properties
#23:38:04,433 ERROR MaxwellBootstrapUtility - failed to connect to mysql server @ jdbc:mysql://shufang101:3306/maxwell
#23:38:04,446 ERROR MaxwellBootstrapUtility - Connections could not be acquired from the underlying database!
#java.sql.SQLException: Connections could not be acquired from the underlying database!
#        at com.mchange.v2.sql.SqlUtils.toSQLException(SqlUtils.java:118)
#        at com.mchange.v2.c3p0.impl.C3P0PooledConnectionPool.checkoutPooledConnection(C3P0PooledConnectionPool.java:692)
#        at com.mchange.v2.c3p0.impl.AbstractPoolBackedDataSource.getConnection(AbstractPoolBackedDataSource.java:140)
#        at com.zendesk.maxwell.util.C3P0ConnectionPool.getConnection(C3P0ConnectionPool.java:18)
#        at com.zendesk.maxwell.bootstrap.MaxwellBootstrapUtility.run(MaxwellBootstrapUtility.java:38)
#        at com.zendesk.maxwell.bootstrap.MaxwellBootstrapUtility.main(MaxwellBootstrapUtility.java:244)
#Caused by: com.mchange.v2.resourcepool.CannotAcquireResourceException: A ResourcePool could not acquire a resource from its primary factory or source.
#        at com.mchange.v2.resourcepool.BasicResourcePool.awaitAvailable(BasicResourcePool.java:1507)
#        at com.mchange.v2.resourcepool.BasicResourcePool.prelimCheckoutResource(BasicResourcePool.java:644)
#        at com.mchange.v2.resourcepool.BasicResourcePool.checkoutResource(BasicResourcePool.java:554)
#        at com.mchange.v2.c3p0.impl.C3P0PooledConnectionPool.checkoutAndMarkConnectionInUse(C3P0PooledConnectionPool.java:758)
#        at com.mchange.v2.c3p0.impl.C3P0PooledConnectionPool.checkoutPooledConnection(C3P0PooledConnectionPool.java:685)
#        ... 4 more
#Caused by: com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure
#
#The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.
#        at com.mysql.cj.jdbc.exceptions.SQLError.createCommunicationsException(SQLError.java:174)
#        at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:64)
#        at com.mysql.cj.jdbc.ConnectionImpl.createNewIO(ConnectionImpl.java:827)
#        at com.mysql.cj.jdbc.ConnectionImpl.<init>(ConnectionImpl.java:447)
#        at com.mysql.cj.jdbc.ConnectionImpl.getInstance(ConnectionImpl.java:237)
#        at com.mysql.cj.jdbc.NonRegisteringDriver.connect(NonRegisteringDriver.java:199)
#        at com.mchange.v2.c3p0.DriverManagerDataSource.getConnection(DriverManagerDataSource.java:175)
#        at com.mchange.v2.c3p0.WrapperConnectionPoolDataSource.getPooledConnection(WrapperConnectionPoolDataSource.java:220)
#        at com.mchange.v2.c3p0.WrapperConnectionPoolDataSource.getPooledConnection(WrapperConnectionPoolDataSource.java:206)
#        at com.mchange.v2.c3p0.impl.C3P0PooledConnectionPool$1PooledConnectionResourcePoolManager.acquireResource(C3P0PooledConnectionPool.java:203)
#        at com.mchange.v2.resourcepool.BasicResourcePool.doAcquire(BasicResourcePool.java:1176)
#        at com.mchange.v2.resourcepool.BasicResourcePool.doAcquireAndDecrementPendingAcquiresWithinLockOnSuccess(BasicResourcePool.java:1163)
#        at com.mchange.v2.resourcepool.BasicResourcePool.access$700(BasicResourcePool.java:44)
#        at com.mchange.v2.resourcepool.BasicResourcePool$ScatteredAcquireTask.run(BasicResourcePool.java:1908)
#        at com.mchange.v2.async.ThreadPoolAsynchronousRunner$PoolThread.run(ThreadPoolAsynchronousRunner.java:696)
#Caused by: com.mysql.cj.exceptions.CJCommunicationsException: Communications link failure
#
#The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.
#        at sun.reflect.GeneratedConstructorAccessor24.newInstance(Unknown Source)
#        at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
#        at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
#        at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:61)
#        at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:105)
#        at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:151)
#        at com.mysql.cj.exceptions.ExceptionFactory.createCommunicationsException(ExceptionFactory.java:167)
#        at com.mysql.cj.protocol.a.NativeProtocol.negotiateSSLConnection(NativeProtocol.java:340)
#        at com.mysql.cj.protocol.a.NativeAuthenticationProvider.negotiateSSLConnection(NativeAuthenticationProvider.java:777)
#        at com.mysql.cj.protocol.a.NativeAuthenticationProvider.proceedHandshakeWithPluggableAuthentication(NativeAuthenticationProvider.java:486)
#        at com.mysql.cj.protocol.a.NativeAuthenticationProvider.connect(NativeAuthenticationProvider.java:202)
#        at com.mysql.cj.protocol.a.NativeProtocol.connect(NativeProtocol.java:1348)
#        at com.mysql.cj.NativeSession.connect(NativeSession.java:163)
#        at com.mysql.cj.jdbc.ConnectionImpl.connectOneTryOnly(ConnectionImpl.java:947)
#        at com.mysql.cj.jdbc.ConnectionImpl.createNewIO(ConnectionImpl.java:817)
#        ... 12 more
#Caused by: javax.net.ssl.SSLHandshakeException: No appropriate protocol (protocol is disabled or cipher suites are inappropriate)
#        at sun.security.ssl.HandshakeContext.<init>(HandshakeContext.java:171)
#        at sun.security.ssl.ClientHandshakeContext.<init>(ClientHandshakeContext.java:101)
#        at sun.security.ssl.TransportContext.kickstart(TransportContext.java:238)
#        at sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:394)
#        at sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:373)
#        at com.mysql.cj.protocol.ExportControlled.performTlsHandshake(ExportControlled.java:316)
#        at com.mysql.cj.protocol.StandardSocketFactory.performTlsHandshake(StandardSocketFactory.java:188)
#        at com.mysql.cj.protocol.a.NativeSocketConnection.performTlsHandshake(NativeSocketConnection.java:99)
#        at com.mysql.cj.protocol.a.NativeProtocol.negotiateSSLConnection(NativeProtocol.java:331)
#        ... 19 more

# 如果在使用maxwell-bootstrap的时候出现以上的问题，只需要将maxwell节点上的jre中的：java.security 文件中的以下内容中对应内容删掉就行了
# https://blog.csdn.net/Wing_kin666/article/details/116449722   参考博客
cd /opt/module/maxwell-1.25.3/
bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table user_info --client_id maxwell_1
bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table base_province --client_id maxwell_1
bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table sku_info --client_id maxwell_1
bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table spu_info --client_id maxwell_1
bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table base_category3 --client_id maxwell_1
bin/maxwell-bootstrap --user maxwell --password 888888 --host shufang101 --database realtime --table base_trademark --client_id maxwell_1