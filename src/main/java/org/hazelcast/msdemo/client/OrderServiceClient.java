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

package org.hazelcast.msdemo.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.hazelcast.msdemo.config.ServiceConfig;
import org.hazelcast.msfdemo.ordersvc.events.OrderGrpc;
import org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreateOrderResponse;

public class OrderServiceClient {
    private static final Logger logger = Logger.getLogger(OrderServiceClient.class.getName());
    private final OrderGrpc.OrderBlockingStub blockingStub; // unused
    private final OrderGrpc.OrderFutureStub futureStub;
    private final OrderGrpc.OrderStub asyncStub;
    private static final int ORDERS_TO_PLACE = 1_000;  // will be 1K or so eventually
    private static int ordersAcknowledged = 0;

    private List<String> validAccounts;

    public static void main(String[] args) {
        ServiceConfig.ServiceProperties props = ServiceConfig.get("order-service");
        String target = props.getTarget();
        logger.info("Target from service.yaml: " + target);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        try {
            OrderServiceClient orderServiceClient = new OrderServiceClient(channel);
            logger.info("Waiting for ordersvc to become ready");
            boolean notReady = true;
            while (notReady) {
                try (Socket ignored = new Socket(props.getGrpcHostname(), props.getGrpcPort())) {
                    break;
                } catch (IOException e) {}
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            }
//            System.out.println("Ordsvc ready, subscribe");
//            orderServiceClient.subscribeToShipmentNotifications();
//            System.out.println("subscribed, send orders");
            orderServiceClient.sendOrders();
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (ordersAcknowledged < ORDERS_TO_PLACE) {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {

            }
        }
        logger.info("All orders acknowledged");

    }

    /** Construct client for accessing server using the existing channel. */
    public OrderServiceClient(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = OrderGrpc.newBlockingStub(channel);
        futureStub = OrderGrpc.newFutureStub(channel);
        asyncStub = OrderGrpc.newStub(channel);
        retrieveAccountInfo();
    }

    private void retrieveAccountInfo() {
        AccountServiceClient asc = new AccountServiceClient();
        try {
            validAccounts = asc.getAllAccountNumbers();
            // Now that dataload is separated out, we don't expect to see empty account list
            if (validAccounts.size() == 0) {
                logger.info("******* UNEXPECTED ********* Initializing 1000 test accounts");
                asc.openTestAccounts(1000);
                validAccounts = asc.getAllAccountNumbers();
                logger.info("After test data init, have " + validAccounts.size() + " accounts");
            } else {
                logger.info("Retrieved info on " + validAccounts.size() + " accounts");

            }
        } catch (StatusRuntimeException e) {
            try {
                logger.info("Waiting for acctsvc to become ready");
                Thread.sleep(1000);
            } catch (InterruptedException interruptedException) {
            }
        }
    }


    public void subscribeToShipmentNotifications() {
        System.out.println("Sending subscribe request for order shipments");
        OrderOuterClass.SubscribeRequest request = OrderOuterClass.SubscribeRequest.newBuilder().build();
        asyncStub.subscribeToOrderShipped(request, new StreamObserver<>() {

            int counter;

            @Override
            public void onNext(OrderOuterClass.OrderShipped orderShipped) {
                counter++;
                System.out.println("Client notified that order " + orderShipped.getOrderNumber() + " has now shipped (#" + counter +")");
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
                // Not expected as we never complete the stream on server side
                System.out.println("Client notified that shipment response stream is completed");
            }
        });
    }

    FutureCallback<CreateOrderResponse> createOrderResponseCallback = new FutureCallback<>() {
        @Override
        public void onSuccess(CreateOrderResponse createOrderResponse) {
            System.out.println("Order created: " + createOrderResponse.getOrderNumber());
            ordersAcknowledged++;
        }

        @Override
        public void onFailure(Throwable throwable) {
            System.out.println("Order creation failed: " + throwable.getMessage());
            throwable.printStackTrace();
        }
    };

    public void sendOrders()  {
        logger.info("Starting OSC.sendOrders");

        // Data generation for inventory might be happening concurrently with the
        // client sending in orders; we'd like to avoid sending orders for items
        // whose inventory records have not yet been created.  Since we generate
        // them sequentially, we can use the size of the inventory map to know
        // what the maximum 'safe' item number is.  When we scale this up, we
        // should periodically refresh the invRecordCount until it reaches
        // max (items * locations, currently 100K)
        //List<ListenableFuture<CreateOrderResponse>> futures = new ArrayList<>();

        InventoryServiceClient iclient = new InventoryServiceClient();
        // This will actually block, logging RetryableHazelcastException, until the load from
        // backing data store has completed.  So we never see partially loaded data.
        System.out.println("Calling getInventoryRecordCount -- might block");
        int invRecordCount = iclient.getInventoryRecordCount();
        System.out.println("Starting to place orders, inventory record count " + invRecordCount);
        int NUM_LOCATIONS = 100;
        int maxSafeItem = invRecordCount / NUM_LOCATIONS;
        for (int i=0; i<ORDERS_TO_PLACE; i++) {
            int index = (int)(Math.random()*validAccounts.size());
            String acctNumber = validAccounts.get(index);
            int itemOffset = (int)(Math.random()*maxSafeItem+1);
            int locationNum = (int)(Math.random()*NUM_LOCATIONS+1);
            String itemNumber = ""+(10101+itemOffset);
            String location = locationNum < 10 ? "W" + locationNum : "S" + locationNum;
            //System.out.println("Account at index " + index + " is " + acctNumber);
             OrderOuterClass.CreateOrderRequest request = OrderOuterClass.CreateOrderRequest.newBuilder()
                    .setAccountNumber(acctNumber)
                    .setItemNumber(itemNumber)
                    .setQuantity(1)
                    .setLocation(location)
                    .build();
            try {
                Executor e = Executors.newCachedThreadPool();
                //futures.add(futureStub.createOrder(request));
                ListenableFuture<OrderOuterClass.CreateOrderResponse> createOrderFuture = futureStub.createOrder(request);
                Futures.addCallback(createOrderFuture,createOrderResponseCallback, e);
                // When changed to server-side streaming RPC, createOrder disappeared from futureStub!
                //asyncStub.createOrder(request, new OrderEventResponseProcessor());
                System.out.println("Placed order " + i);
            } catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                return;
            }
        }

        return;
    }
}
