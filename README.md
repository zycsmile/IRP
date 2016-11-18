# IRP
Created by Yuchao

Many large companies provide various services with their distributed datacenters (DC), and the back-end data of these services is mixstored in the thousands of servers in each DC. When traffic bursts, some servers become very “hot” due to the imbalance of data storage, making the response time of these requests much longer than usual. Existing solutions try to solve this problem by rescheduling requests and achieve load balance, but always failed when the back-end data are fixed stored in some particular servers.

In this project, we try to solve the Index Rebalance Problem (IRP), and attempt to eliminate hot machines and balance the usage of all servers. We design a prebalance algorithm that violates the balance principle but makes significant contribution to the following rebalance procedure, and then employ a multi-dimensional local search algorithm.
