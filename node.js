// Load the Snowflake Node.js driver.
var snowflake = require('snowflake-sdk');

// Create a Connection object that we can use later to connect.
var connection = snowflake.createConnection({
    account: account,
    username: user,
    password: password,
    application: application
  });

// Try to connect to Snowflake, and check whether the connection was successful.
connection.connect( 
    function(err, conn) {
        if (err) {
            console.error('Unable to connect: ' + err.message);
            } 
        else {
            console.log('Successfully connected to Snowflake.');
            // Optional: store the connection ID.
            connection_ID = conn.getId();
            }
    }
);

// Define the SQL query to fetch relevant data
const sqlQuery = `
SELECT
     VP."User ID",
     VP."PROFILE UUID",
     VP."USERNAME",
     VP."Status",
     VAB."EMAIL",
     VAB."TIER"
FROM
     "VIEW_PROFILES" AS VP
JOIN
     "VIEW_ACCOUNT_BILLING" AS VAB
ON
     VP."User ID" = VAB."USER ID"
     AND VP."PROFILE UUID" = VAB."PROFILE UUID"
WHERE
     VP."USERNAME" = ?
     AND VP."Status" = 'Active'
     AND VAB."Tier" = 'Paid';
 `;

 // Execute the SQL query
 const statement = await snowflakeConnection.execute({
   sqlText: sqlQuery,
   binds: [username]
 });

 // Fetch the result
 const resultSet = await statement.fetchAll();

 if (resultSet.length === 0) {
   console.log('No paid active user found with the username:', username);
   return null;
 }

 // Extract identifiers from Snowflake result
 const { "User ID": userId, "PROFILE UUID": profileUUID } = resultSet[0];

//Require the Chargebee Library
var chargebee = require('chargebee');

//Connect to the API
chargebee.configure({site : "{site}",
  api_key : "{site_api_key}"})

  const ChargeBee = require('chargebee');

  // Initialize Chargebee with your API key and site
  ChargeBee.configure({
    site: 'your_site',
    api_key: 'your_api_key'
  });
  
  // Iterate over the joined dataset
  joinedDataset.forEach(async (row) => {
    try {
      const username = row['USERNAME']; //Our identifier in Chargebee to fetch customer records
  
      // Retrieve customer details
      const customerResult = await ChargeBee.customer.retrieve(username);
      const customer = customerResult.customer;
  
      // Retrieve subscription details
      const subscriptionResult = await ChargeBee.subscription.retrieve(username);
      const subscription = subscriptionResult.subscription;

      //Retrieve item price details
      const item_priceResult = await ChargeBee.item_price.retrieve(username);
      const item_price = item_priceResult.item_price;

      // Retrieve transactions price details
      const item_transactionsResult = await ChargeBee.item_transactions.retrieve(username);
      const item_transactons = item_transactionsResult.item_transactions;

      // Extract required attributes
      const firstName = customer.first_name;
      const lastName = customer.last_name;
      const startedAt = subscription.started_at;
      const cancelledAt = subscription.cancelled_at;
      const cancelReason = subscription.cancel_reason;
      const currencyCode = item_price.currency_code;
      const totalDues = subscription.total_dues;
      const paymentamount = item_price.amount
      const subsscriptiontier = item_price.tier      
      const externalname = item_price.external_name
      const paymentfrequency = item_price.frequency
  
      // Process or store the retrieved attributes as needed
      console.log('Username', username);
      console.log('First Name:', first_name);
      console.log('Last Name:', last_ame);
      console.log('Subscription Start Date:', started_at);
      console.log('Subscription Cancelled:', cancelled_at);
      console.log('Reason for Cancelling:', cancel_reason);
      console.log('Currency Type:', currency_code);
      console.log('Total Invoice Due Amount for Subscription:', total_dues);
    } catch (error) {
      console.error('Error retrieving data for username', username, ':', error);
    }
  });

  // Extract relevant attributes
  const billingInfo = {
    username: customer.username,
    email: customer.email,
    paymentamount: subscription.plan_amount,
    startedAt: subscription.created_at,
    cancelledAt: subscription.cancelled_at,
    cancelReason: subscription.cancel_reason,
    currencyCode: subscription.currency_code,
    totalDues: subscription.total_dues
  };

  return billingInfo;

// Use the billingInfo object to populate your Retool table