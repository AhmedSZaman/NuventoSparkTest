package org.nuvento.exam.model

/**
 *
 * @param customerID unique customerID
 * @param accountID unique accountID
 * @param balance balance in the particular account
 */
case class accountModel(customerID: String, accountID: String, balance: Int)

/**
 *
 * @param customerID unique customerID
 * @param forename forename of customer
 * @param surname surname of customer
 */
case class customerModel(customerID: String, forename: String, surname: String)

/**
 *
 * @param customerID unique customerID
 * @param forename forename of customer
 * @param surname surname of customer
 * @param accounts array of all accounts linked to customer
 * @param totalBalance total balance of all linked accounts
 * @param numberAccounts the number of linked accounts to customer
 * @param averageBalance average balance across all linked accounts
 */
case class customerAccountModel(customerID: String, forename: String, surname: String,accounts: Array[String],
                               totalBalance: Long, numberAccounts: Long, averageBalance: Double  )

/**
 *
 * @param addressID unique address identifier
 * @param customerID unique customerID
 * @param address unparsed address
 */
case class addressModel(addressID:String, customerID: String, address:String)

/**
 *
 * @param customerID unique customerID
 * @param streetNumber street number of address field
 * @param streetName street name of the address field
 * @param city city name of address field
 * @param country country name of address field
 */
case class parsedAddressModel(customerID: String,streetNumber: String, streetName: String, city: String, country:String)

/**
 *
 * @param customerID unique customerID
 * @param forename forename of customer
 * @param surname surname of customer
 * @param accounts array of all accounts linked to customer
 * @param streetNumber street number of address field
 * @param streetName street name of the address field
 * @param city city name of address field
 * @param country country name of address field
 */
case class customerDocumentModel(customerID: String, forename: String, surname: String,accounts: Array[String],
                                streetNumber: String, streetName: String, city: String, country:String)