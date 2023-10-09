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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.hazelcast.msdemo.config.ServiceConfig;
import org.hazelcast.msfdemo.acctsvc.events.AccountGrpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.hazelcast.msfdemo.acctsvc.events.AccountOuterClass.*;

/** Note that this has morphed into a dual-purpose client
 *  - When run standalone, it's a test for the Account service, and it tests the service
 *    by generating test data and then doing a bunch of test transfers.  This is all
 *    initiated from the main() method.
 *  - But this is also used to support the OrderServiceClient, and specifically by
 *    querying the account service to get a list of valid accounts.  This is done via
 *    the public getAccountNumbers() method.
 */
public class AccountServiceClient {

    private static final Logger logger = Logger.getLogger(AccountServiceClient.class.getName());
    private final AccountGrpc.AccountBlockingStub blockingStub;
    private final AccountGrpc.AccountFutureStub futureStub;
    private ManagedChannel channel;

    private List<String> accountNumbers = new ArrayList<>();
    private static final int OPEN_THREAD_COUNT = 10;
    private static final int TRANSFER_THREAD_COUNT = 1;
    private static final int ACCOUNT_COUNT = 1000;

    // Restore to 10K transfers once working
    private static final int TRANSFER_COUNT = 10_000; // 100K will run out of native threads in gRPC

    private ManagedChannel initChannel() {
        ServiceConfig.ServiceProperties props = ServiceConfig.get("account-service");
        String target = props.getTarget();
        logger.info("Target from service.yaml " + target);

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

    public void openTestAccounts(int number) {
        if (channel == null)
            initChannel();

        List<Thread> openWorkers = new ArrayList<>();
        int accountsPerThread = number / OPEN_THREAD_COUNT;
        for (int i=0; i<OPEN_THREAD_COUNT; i++) {
            Thread t = new Thread(new OpenRunnable("B0" + i, accountsPerThread));
            openWorkers.add(t);
            t.start();
        }

        for (Thread t : openWorkers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

//    public List<String> getAllAccountNumbers() {
//        return accountNumbers;
//    }

    public List<String> getAllAccountNumbers() {
        ConnectivityState state = channel.getState(true);
        // Can treat IDLE, CONNECTING, READY as good to go
        while( state != ConnectivityState.READY) {
            try {
                Thread.sleep(10_000);
                logger.info("Waiting on acctsvc to be ready, channel state " + state.toString());
                state = channel.getState(true);

            } catch (InterruptedException e) {
                // OK
            }
        }
        //logger.info("*** Connected to account service");
        AllAccountsRequest request = AllAccountsRequest.newBuilder().build();
        AllAccountsResponse response = blockingStub.allAccountNumbers(request);
        accountNumbers = response.getAccountNumberList();
        return accountNumbers;
    }

    public int getNumberOfAccounts() {
        // Refresh the list in case it has changed
        getAllAccountNumbers();
        return accountNumbers.size();
    }

    public static void main(String[] args) throws Exception {
        //ManagedChannel channel = initChannel();
        AccountServiceClient accountServiceClient = new AccountServiceClient();

        try {
            // Open 1000 accounts
            logger.info("Opening 1000 accounts using " + OPEN_THREAD_COUNT + " threads");
            long start = System.currentTimeMillis();
            accountServiceClient.openTestAccounts(ACCOUNT_COUNT);
            long elapsed = System.currentTimeMillis() - start;
            logger.info("Opened accounts multi threaded non-blocking in " + elapsed + "ms");
            logger.info("Number of account numbers opened : " + accountServiceClient.accountNumbers.size());

            long totalBalanceAfterOpens =  accountServiceClient.getTotalBalances();
            logger.info("Total balance across accounts: " + accountServiceClient.getTotalBalances());

            // Test transfer money between accounts
            logger.info("Performing transfers");
            start = System.currentTimeMillis();
            // Non-blocking, single threaded
            int successCount = accountServiceClient.nonBlockingTransfer("A", TRANSFER_COUNT);
            elapsed = System.currentTimeMillis() - start;
            logger.info("Finished " + successCount + " transfers using single-threaded non-blocking in " + elapsed + "ms");

            long totalBalanceAfterTransfers =  accountServiceClient.getTotalBalances();
            logger.info("Total balance across accounts : " + totalBalanceAfterTransfers);
            if (totalBalanceAfterOpens == totalBalanceAfterTransfers)
                logger.info("It's all good.");
            else
                logger.info("Out of balance by " + (totalBalanceAfterOpens - totalBalanceAfterTransfers));

            logger.info("Finished, disconnecting");
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            accountServiceClient.shutdownChannel();
        }
    }

    public long getTotalBalances() {
        TotalBalanceRequest request = TotalBalanceRequest.newBuilder().build();
        long totalBalance = blockingStub.totalAccountBalances(request).getTotalBalance();
        return totalBalance;
    }


    /** Construct client for accessing server using the existing channel. */
    public AccountServiceClient() {
        channel = initChannel();

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = AccountGrpc.newBlockingStub(channel);
        futureStub = AccountGrpc.newFutureStub(channel);
    }

    // Blocking version on open, not used any longer
    public String open(String name, int balance) {
        //logger.info("Opening account");
        OpenAccountRequest request = OpenAccountRequest.newBuilder()
                .setAccountName(name)
                .setInitialBalance(balance)
               .build();
        OpenAccountResponse response;
        try {
            response = blockingStub.open(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        //logger.info("Opened account: " + response.getAccountNumber());
        return response.getAccountNumber();
    }

    /* Non-blocking runnable uses prefix so account names don't repeat between threads */
    class OpenRunnable implements Runnable {
        final String prefix;
        final int accountsToOpen;
        public OpenRunnable(String prefix, int accounts) {
            this.prefix = prefix;
            this.accountsToOpen = accounts;
        }
        @Override
        public void run() {
            List<String> openedAccounts = nonBlockingOpen(prefix, accountsToOpen);
            accountNumbers.addAll(openedAccounts);
            //logger.info("Open worker " + prefix + " finished opening " + openedAccounts.size() + " accounts");
        }
    }

    public List<String> nonBlockingOpen(String prefix, int accountsToOpen)  {

        // This should probably be implemented on all services, but since this is the
        // first thing we call just putting it here for now.
        ConnectivityState state = channel.getState(true);
        // Can treat IDLE, CONNECTING, READY as good to go
        while( state != ConnectivityState.READY) {
            try {
                Thread.sleep(10_000);
                //logger.info("Waiting on acctsvc to be ready, channel state " + state.toString());
                state = channel.getState(true);

            } catch (InterruptedException e) {
                // OK
            }
        }
        //logger.info("*** Connected to account service");

        List<ListenableFuture<OpenAccountResponse>> futures = new ArrayList<>();
        for (int i=0; i<accountsToOpen; i++) {
            String name = "Acct " + prefix + i;
            int balance = Double.valueOf(Math.random()*10000).intValue()*100;
            OpenAccountRequest request = OpenAccountRequest.newBuilder()
                    .setAccountName(name)
                    .setInitialBalance(balance)
                    .build();
            try {
                futures.add(futureStub.open(request));
            } catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                return null;
            }
        }
        //System.out.println("Submitted " + n + " open account requests for group " + prefix);

        try {
            // Can use successfulAsList rather than allAsList to get only good responses
            ListenableFuture<List<OpenAccountResponse>> responseList = Futures.successfulAsList(futures);
            List<OpenAccountResponse> responses = responseList.get();
            List<String> openedAccountNumbers = new ArrayList<>();
            //logger.info("Successful response count for group " + prefix + " = " + responses.size());
            for (OpenAccountResponse oar : responses) {
                openedAccountNumbers.add(oar.getAccountNumber());
                // Bad results not coming at end, can be anywhere in the list
                if (oar.getAccountNumber() == null || oar.getAccountNumber().trim().equals("")) {
                    logger.info("OpenAccountRequest received bad result :"+ oar.getAccountNumber() + ":");
                }
            }
            return openedAccountNumbers;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    public int adjust(String acctNumber, int amount) {
        boolean debit = amount < 0;
        if (debit) amount = -amount;
        AdjustBalanceRequest request = AdjustBalanceRequest.newBuilder()
                .setAccountNumber(acctNumber)
                .setAmount(amount)
                .build();
        AdjustBalanceResponse response;
        try {
            if (debit)
                response = blockingStub.withdraw(request);
            else
                response = blockingStub.deposit(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return 0;
        }

        //logger.info("adjusted account: " + acctNumber + " new balance is " + response.getNewBalance());
        return response.getNewBalance();
    }

    public int checkBalance(String acctNumber) {
        CheckBalanceRequest request = CheckBalanceRequest.newBuilder()
                .setAccountNumber(acctNumber)
                .build();
        CheckBalanceResponse response;
        try {
            response = blockingStub.checkBalance(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return 0;
        }

        return response.getBalance();
    }

    public boolean transfer(String fromAcct, String toAcct, int amount) {

        TransferMoneyRequest request = TransferMoneyRequest.newBuilder()
                .setFromAccountNumber(fromAcct)
                .setToAccountNumber(toAcct)
                .setAmount(amount).build();
        TransferMoneyResponse response;
        try {
            //logger.info("Calling blocking stub for transfer");
            response = blockingStub.transferMoney(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return false;
        }
        return response.getSucceeded();
    }

    /* Non-blocking runnable uses prefix so account names don't repeat between threads */
    class TransferRunnable implements Runnable {
        final String prefix;
        final int numTransfers;
        public TransferRunnable(String prefix, int numTransfers) {
            this.prefix = prefix;
            this.numTransfers = numTransfers;
        }
        @Override
        public void run() {
            int transfersCompleted = nonBlockingTransfer(prefix, numTransfers);
            logger.info("Transfer worker " + prefix + " finished " + transfersCompleted + " transfers");
        }
    }

    // Returns the number of successful transfers
    public int nonBlockingTransfer(String prefix, int numTransfers) {
        logger.info("Transfer handler " + prefix + " initiating " + numTransfers + " transfers");

        List<ListenableFuture<TransferMoneyResponse>> futures = new ArrayList<>();

        for (int i=0; i<numTransfers; i++) {
            int fromIndex = Double.valueOf(Math.random()*100).intValue();
            int toIndex = Double.valueOf(Math.random()*100).intValue();
            while (toIndex == fromIndex) {
                // recalc
                toIndex = Double.valueOf(Math.random()*100).intValue();
            }
            String fromAcct = accountNumbers.get(fromIndex);
            String toAcct = accountNumbers.get(toIndex);
            TransferMoneyRequest request = TransferMoneyRequest.newBuilder()
                    .setFromAccountNumber(fromAcct)
                    .setToAccountNumber(toAcct)
                    .setAmount(1234) // 12.34, implied 2 decimal places
                    .build();
            try {
                futures.add(futureStub.transferMoney(request));
            } catch (StatusRuntimeException e) {
                logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
                return 0;
            }
        }

        // Can use successfulAsList rather than allAsList to get only good responses
        logger.info("Consolidating futures for " + futures.size() + " transfer requests");
        ListenableFuture<List<TransferMoneyResponse>> responseList = Futures.allAsList();
        List<TransferMoneyResponse> responses = null;
        try {
            logger.info("Getting responses");
            responses = responseList.get(1, TimeUnit.MINUTES);
            logger.info("Successful transfer response count for group " + prefix + " = " + responses.size());
            return responses.size();
        } catch (TimeoutException te) {
            logger.info("Timed out after 1 minute with " + responses.size() + " responses");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
