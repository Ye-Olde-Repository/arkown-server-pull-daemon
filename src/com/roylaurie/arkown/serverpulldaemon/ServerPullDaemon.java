package com.roylaurie.arkown.serverpulldaemon;
import java.sql.SQLException;
import java.util.List;

import com.roylaurie.arkown.engine.Connection.ConnectionException;
import com.roylaurie.arkown.engine.Query.QueryException;
import com.roylaurie.arkown.server.Server;
import com.roylaurie.arkown.server.sql.ServerSql;
import com.roylaurie.modelsql.ModelSql;
import com.roylaurie.modelsql.ModelSql.ColumnQueryList;

/**
 * Copyright (C) 2011 Roy Laurie Software <http://www.roylaurie.com>
 */

/**
 * @author Roy Laurie <roy.laurie@roylaurie.com> RAL
 *
 */
public final class ServerPullDaemon {
    private static final int PULL_BATCH_SIZE = 100;
    private static final long SERVER_PULL_INTERVAL_SECONDS = 7;
    private static final long SERVER_EXPIRED_TIMEOUT_SECONDS = 172800; // 48 hours
    private static final ServerPullDaemon sSingleton = new ServerPullDaemon();
    
    
    private final class Puller implements Runnable {
        private List<Server> mServerList = null;
        private long mExpirationTime = 0;
        
        private Puller(List<Server> serverList, long expirationTime) {
            mServerList = serverList;
            mExpirationTime = expirationTime;
        }
        
        public void run() {
            ServerSql serverSql = new ServerSql(ModelSql.CONTEXT_WRITE);
            
            for (Server server : mServerList) {
                try {            
                    server.pull();
                } catch (ConnectionException e) {
                    e.printStackTrace();
                    if (server.getLastPullTime() <= mExpirationTime) {
                        try {
                            serverSql.delete(server);
                        } catch (SQLException e1) {
                            e1.printStackTrace();
                        }
                    }
                    continue;
                } catch (QueryException e) {
                    e.printStackTrace();
                    if (server.getLastPullTime() <= mExpirationTime) {
                        try {
                            serverSql.delete(server);
                        } catch (SQLException e1) {
                            e1.printStackTrace();
                        }
                    }                
                    continue;
                }
                
                try {
                    serverSql.write(server);
                } catch (SQLException e) {
                    e.printStackTrace();
                    continue;
                }
            }   
        }
    }       
    
    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        sSingleton.initialize();
        sSingleton.run();
    }
    
    public void initialize() {
        ModelSql.setConfigurationFilepath("res/xml/com/roylaurie/sql/arkown.xml");        
    }
    
    public void run() {
        while(true) {
            //heartbeat();
            
            try {
                pull();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            
            //heartbeat();
            sleep();
        }        
    }
    
    private void sleep() {
        try {
            Thread.sleep(SERVER_PULL_INTERVAL_SECONDS * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }         
    }
 
    
    private void pull() throws SQLException {
        long lastPullTime = System.currentTimeMillis() - SERVER_PULL_INTERVAL_SECONDS;
        long expiredPullTime = System.currentTimeMillis() - SERVER_EXPIRED_TIMEOUT_SECONDS;
        ServerSql serverSql = new ServerSql(ModelSql.CONTEXT_READ);
        ColumnQueryList queryList = new ColumnQueryList();
        List<Server> results = null;
        int offset = 0;
        
        queryList.and(ServerSql.Column.LAST_PULL_TIME, "<=", lastPullTime); 
        
        while(true) {
            results = serverSql.find(queryList, PULL_BATCH_SIZE, offset);
            if (results.isEmpty()) {
                break;
            }
         
            Thread pullerThread = new Thread(new Puller(results, expiredPullTime));
            pullerThread.start();
            offset += results.size();
        }
    }    
}
