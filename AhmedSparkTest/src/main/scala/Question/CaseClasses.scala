package Question

case class accountData(customerID: String, accountID: String, balance: Int)
case class customerData(customerID: String, forename: String, surname: String)

case class customerAccountData(customerID: String, forename: String, surname: String,accounts: Array[String],
                               totalBalance: Long, numberAccounts: Long, averageBalance: Double )
case class addressData(addressId:String, customerId: String, address:String)
case class parsedAddressData(streetNumber: String, streetName: String, city: String, country:String)

case class customerDocumentData(customerID: String, forename: String, surname: String,accounts: Array[String],
                                streetNumber: String, streetName: String, city: String, country:String)