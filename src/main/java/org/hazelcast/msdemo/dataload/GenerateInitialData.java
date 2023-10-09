package org.hazelcast.msdemo.dataload;

import org.hazelcast.msdemo.client.AccountServiceClient;

import java.util.logging.Logger;

public class GenerateInitialData {
    private static final Logger logger = Logger.getLogger(GenerateInitialData.class.getName());

    public static void main(String[] args) {
        GenerateInitialData main = new GenerateInitialData();
        main.createAccounts();
        main.createInventory();
    }

    public GenerateInitialData() { }

    private void createAccounts() {
        AccountServiceClient asc = new AccountServiceClient();
        try {
            // Expect no accounts initially, but check in case we later make account info
            // persistent.
            int validAccounts = asc.getNumberOfAccounts();
            if (validAccounts == 0) {
                logger.info("Initializing 1000 test accounts");
                asc.openTestAccounts(1000);
                validAccounts = asc.getNumberOfAccounts();
                logger.info("After test data init, have " + validAccounts + " accounts");
            } else {
                logger.info("Account info was already initialized for " + validAccounts + " accounts");
            }
        } catch (io.grpc.StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    private void createInventory() {
        InventoryDataGenerator igen = new InventoryDataGenerator();
        int currentItemCount = igen.getItemCount();
        System.out.println("Current item count is " + currentItemCount);
        int currentInventoryCount = igen.getInventoryCount();
        System.out.println("Current inventory record count is " + currentInventoryCount);
        if (currentItemCount == 0) {
            try {
                logger.info("Initializing 1000 inventory items");
                igen.createItems(1000);
                // create items will trigger the associated inventory records creation
                logger.info("Inventory items initialized");
                igen.createStock();
                logger.info("Inventory stock records initialized");

                while (! igen.dataloadFinished()) {
                    System.out.println("DataLoadMain waiting for inventory load to finish ");
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Items already present, nothing generated");
        }
    }
}
