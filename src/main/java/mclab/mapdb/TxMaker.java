/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package mclab.mapdb;


import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Transaction factory
 *
 * @author Jan Kotek
 */
public class TxMaker implements Closeable {

    private final boolean strictDBGet;
    protected ScheduledExecutorService executor;

    /** parent engine under which modifications are stored */
    protected Engine engine;

    public TxMaker(Engine engine) {
        this(engine,false,false, null);
    }

    public TxMaker(Engine engine, boolean strictDBGet, boolean txSnapshotsEnabled, ScheduledExecutorService executor) {
        if(engine==null) throw new IllegalArgumentException();
        if(!engine.canSnapshot())
            throw new IllegalArgumentException("Snapshot must be enabled for TxMaker");
        if(engine.isReadOnly())
            throw new IllegalArgumentException("TxMaker can not be used with read-only Engine");
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.executor = executor;
    }

    
    public DB makeTx(){
        Engine snapshot = engine.snapshot();
        if(snapshot.isReadOnly())
            throw new AssertionError();
//        if(txSnapshotsEnabled)
//            snapshot = new TxEngine(snapshot,false); //TODO
        return new DB(snapshot,strictDBGet,false,executor, true, null, 0, null, null);
    }

    public void close() {
        engine.close();
        engine = null;
    }

    /**
     * Executes given block withing single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * @param txBlock
     */
    public void execute(TxBlock txBlock) {
        for(;;){
            DB tx = makeTx();
            try{
                txBlock.tx(tx);
                if(!tx.isClosed())
                    tx.commit();
                return;
            }catch(TxRollbackException e){
                //failed, so try again
                if(!tx.isClosed()) tx.close();
            }
        }
    }

    /**
     * Executes given block withing single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * This method returns result returned by txBlock.
     *
     * @param txBlock
     */
    public <A> A execute(Fun.Function1<A, DB> txBlock) {
        for(;;){
            DB tx = makeTx();
            try{
                A a = txBlock.run(tx);
                if(!tx.isClosed())
                    tx.commit();
                return a;
            }catch(TxRollbackException e){
                //failed, so try again
                if(!tx.isClosed()) tx.close();
            }
        }
    }
}
