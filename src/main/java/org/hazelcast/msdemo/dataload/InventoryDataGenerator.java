/*
 * Copyright 2018-2022 Hazelcast, Inc
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

package org.hazelcast.msdemo.dataload;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.hazelcast.msdemo.config.ServiceConfig;
import org.hazelcast.msfdemo.invsvc.events.InventoryGrpc;
import org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass;
import org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass.ClearAllDataRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class InventoryDataGenerator {

    private static final Logger logger = Logger.getLogger(InventoryDataGenerator.class.getName());
    private final InventoryGrpc.InventoryBlockingStub blockingStub;
    private final InventoryGrpc.InventoryStub nonblockingStub;
    private ManagedChannel channel;
    private boolean allBatchesFinished = false;
    // Assume single-threaded here; if multi then we need to have one of these per thread
    private boolean currentBatchFinished = false;

    private static String[] categories = new String[] {
            "Books", "Music", "Furniture", "Sporting Goods", "Men's Clothing",
            "Women's Clothing", "Men's Shoes", "Women's Shoes", "Toys", "Collectibles",
            "Kitchen", "Tools", "Electronics", "Computers", "Pet Supplies",
            "Groceries", "Gardening", "Health", "Cosmetics", "Accessories",
            "Jewelry", "Camera & Photo", "Automotive", "Antiques", "Appliances" };

    private static final int ITEM_THREAD_COUNT = 1;
    private static final int INVENTORY_THREAD_COUNT = 1; // was 10

    private int items; // passed in
    private int locationsPerItem = 100;
    private int warehousesPerItem = 10; // locationsPerItem - whPerItem = storesPerItem
    private int maxInFlightRequests = 10000;
    private int currentInFlightRequests = 0;

    public boolean dataloadFinished() {
        return allBatchesFinished;
    }

    private ManagedChannel initChannel() {
        ClassLoader cl = InventoryDataGenerator.class.getClassLoader();
        ServiceConfig.ServiceProperties props = ServiceConfig.get("service.yaml", "inventory-service", cl);
        String target = props.getTarget();
        logger.info("Target from dataload.yaml " + target);

        channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        return channel;
    }

    private void shutdownChannel() {
        try {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            ;
        }
    }

    public void resetData() {
        ClearAllDataRequest request = ClearAllDataRequest.newBuilder().build();
        blockingStub.clearAllData(request);
        // Because we're write behind, even though this blocks we can still have
        // data in MySQL when it returns
        System.out.println("clearAllData (should be blocking) complete");
    }

    // TODO: not doing multi-threaded creation here, can avoid the whole
    //  runnable thing -- probably just expose bulkCreateItems as the API
    public void createItems(int number) {
        this.items = number;
        List<Thread> itemWorkers = new ArrayList<>();
        int itemsPerThread = number / ITEM_THREAD_COUNT;
        for (int i=0; i<ITEM_THREAD_COUNT; i++) {
            Thread t = new Thread(new ItemRunnable(itemsPerThread));
            itemWorkers.add(t);
            t.start();
        }

        for (Thread t : itemWorkers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /** Construct client for accessing server using the existing channel. */
    public InventoryDataGenerator() {
        channel = initChannel();
        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = InventoryGrpc.newBlockingStub(channel);
        nonblockingStub = InventoryGrpc.newStub(channel);
    }

    ////////////
    // ITEMS
    ////////////

    public int getItemCount() {
        InventoryOuterClass.ItemCountRequest request =
                InventoryOuterClass.ItemCountRequest.newBuilder().build();
        InventoryOuterClass.ItemCountResponse response = blockingStub.getItemCount(request);
        return response.getCount();
    }

    class ItemRunnable implements Runnable {
        final int itemsToCreate;
        public ItemRunnable(int items) {
            this.itemsToCreate = items;
        }
        @Override
        public void run() {
            bulkCreateItems(itemsToCreate);
        }
    }

    public void bulkCreateItems(int itemsToCreate)  {
        // This should probably be implemented on all services, but since this is the
        // first thing we call just putting it here for now.
        ConnectivityState state = channel.getState(true);
        // Can treat IDLE, CONNECTING, READY as good to go
        while( state != ConnectivityState.READY) {
            try {
                Thread.sleep(10_000);
                logger.info("Waiting on invsvc to be ready, channel state " + state.toString());
                state = channel.getState(true);

            } catch (InterruptedException e) {
                // OK
            }
        }
        logger.info("*** Connected to inventory service");

        StreamObserver<InventoryOuterClass.AddItemResponse> response =
                new StreamObserver<>() {

                    @Override
                    public void onNext(InventoryOuterClass.AddItemResponse addItemResponse) {
                        // Currently returns empty message, maybe just return a simple count
                        // of items?
                        System.out.println("********************** IDG: Next AddItemResponse received");
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("IDG: AddItemReponse.onError:");
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("*********************** IDG: AddItems complete");
                    }
                };

        StreamObserver<InventoryOuterClass.AddItemRequest> requestObserver =
                nonblockingStub.addItem(response);

        int initialItemNumber = 10101;
        for (int i=0; i<itemsToCreate; i++) {
            String itemNumber = "" + initialItemNumber++;
            int catindex = (int) (Math.random() * categories.length);

            InventoryOuterClass.AddItemRequest request =
                    InventoryOuterClass.AddItemRequest.newBuilder()
                            .setItemNumber(itemNumber)
                            .setDescription("Item " + itemNumber)
                            .setCategoryID("C" + catindex)
                            .setCategoryName(categories[catindex])
                            .setPrice((int) (Math.random() * 10000))
                            .build();

            requestObserver.onNext(request);
        }
        System.out.println("Finished item creation ... calling onCompleted");
        requestObserver.onCompleted();
    }

    ////////////
    // INVENTORY
    ////////////

    public void createStock() {
        List<Thread> stockWorkers = new ArrayList<>();
        int startingItem = 10101;
        int itemsPerThread = items / INVENTORY_THREAD_COUNT;
        for (int i=0; i<INVENTORY_THREAD_COUNT; i++) {
            Thread t = new Thread(new InventoryRunnable(startingItem, itemsPerThread));
            stockWorkers.add(t);
            t.start();
            startingItem += itemsPerThread;
        }

        System.out.println("IDG: Waiting for stock workers to finish");
        for (Thread t : stockWorkers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("IDG: Stock workers finished");
    }


    public int getInventoryCount() {
        InventoryOuterClass.InventoryCountRequest request =
                InventoryOuterClass.InventoryCountRequest.newBuilder().build();
        InventoryOuterClass.InventoryCountResponse response = blockingStub.getInventoryRecordCount(request);
        return response.getCount();
    }

    class InventoryRunnable implements Runnable {
        //final InventoryOuterClass.AddItemRequest addItemRequest;
        final int startingItem;
        final int itemCount;
        public InventoryRunnable(int start, int count) {
            this.startingItem = start;
            this.itemCount = count;
        }
        @Override
        public void run() {
            bulkCreateInventory(startingItem, itemCount);
        }
    }

    public void bulkCreateInventory(int startingItem, int itemCount)  {
        //int currentItem = startingItem;
        int maxItem = startingItem + itemCount;
        System.out.println("Thread " + Thread.currentThread().getName() + " creating " + startingItem + "-" + (startingItem + itemCount) );
        StreamObserver<InventoryOuterClass.AddInventoryResponse> responseObserver =
                new StreamObserver<>() {

                    @Override
                    public void onNext(InventoryOuterClass.AddInventoryResponse addInventoryResponse) {
                        int ackCount = addInventoryResponse.getAckCount();
                        currentInFlightRequests -= ackCount;
                        //System.out.println("IDG: Received ack of " + ackCount + " requests, in flight now " + currentInFlightRequests);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("IDG: AddInventoryReponse.onError:");
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("IDG AddInventory complete for " + startingItem + "-" + maxItem);
                        // Currently we only get ack after all items, may change to a streaming
                        // response with a max-in-flight setting for flow control
                        allBatchesFinished = true;
                    }
                };

        StreamObserver<InventoryOuterClass.AddInventoryRequest> requestObserver =
                nonblockingStub.addInventory(responseObserver);


        for (int item=startingItem; item<maxItem; item++) {
            for (int lidx = 0; lidx < locationsPerItem; lidx++) {
                while (currentInFlightRequests > maxInFlightRequests) {
                    System.out.println("Pausing: in flight " + currentInFlightRequests + " exceeds threshold " + maxInFlightRequests);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                int qty = (int) (Math.random() * 1000);
                String location = lidx < warehousesPerItem ? "W" + lidx : "S" + lidx;
                String locType = lidx < warehousesPerItem ? "WH" : "S";
                InventoryOuterClass.AddInventoryRequest request =
                        InventoryOuterClass.AddInventoryRequest.newBuilder()
                                .setItemNumber("" + item)
                                .setDescription("Item " + item)
                                .setLocation(location)
                                .setLocationType(locType)
                                .setGeohash("FUTURE")
                                .setQtyOnHand(qty)
                                .setQtyReserved(0)
                                .setAvailToPromise(qty)
                                .build();
                currentInFlightRequests++;
                requestObserver.onNext(request);
            }
        }
        System.out.println("Thread " + Thread.currentThread().getName() + " finished " + startingItem + "-" + (startingItem + itemCount) + ", calls onCompleted" );
        requestObserver.onCompleted();
    }
}
