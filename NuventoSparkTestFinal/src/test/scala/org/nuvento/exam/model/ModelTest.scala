package org.nuvento.exam.model

import org.scalatest.funsuite.AnyFunSuite
class ModelTest extends AnyFunSuite {


  test("accountModel") {
    val account = accountModel("12345", "ACC001", 1000)

    val unapplyResult = accountModel.unapply(account)

    unapplyResult match {
      case Some((customerID, accountID, balance)) =>
        assert(customerID == "12345" )
        assert(accountID == "ACC001")
        assert(balance == 1000)
    }
  }
  test("customerModel") {
    val customer = customerModel("12345", "John", "Snow")

    val unapplyResult = customerModel.unapply(customer)
    unapplyResult match {
      case Some((customerID, forename, surname)) =>
        assert(customerID == "12345")
        assert(forename == "John")
        assert(surname == "Snow")
    }
  }

  test("customerAccountModel") {
    val accounts = Array("ACC001", "ACC002")
    val customerAccount = customerAccountModel("12345", "John", "Snow", accounts, 1000, 2, 500.0)

    val unapplyResult = customerAccountModel.unapply(customerAccount)
    unapplyResult match {
      case Some((customerID, forename, surname, accounts, totalBalance, numberAccounts, averageBalance)) =>
        assert(customerID == "12345")
        assert(forename == "John")
        assert(surname == "Snow")
        assert(accounts.sameElements(accounts))
        assert(totalBalance == 1000)
        assert(numberAccounts == 2)
        assert(averageBalance == 500.0)
    }
  }

  test("addressModel") {
    val address = addressModel("AD01", "C005", "123 Wall St")

    val unapplyResult = addressModel.unapply(address)
    unapplyResult match {
      case Some((addressID, customerID, address)) =>
        assert(addressID == "AD01")
        assert(customerID == "C005")
        assert(address == "123 Wall St")
    }
  }

  test("parsedAddressModel") {
    val parsedAddress = parsedAddressModel("AC001","123", "Winter St", "Wall", "Winterfell")

    val unapplyResult = parsedAddressModel.unapply(parsedAddress)
    unapplyResult match {
      case Some((customerID,streetNumber, streetName, city, country)) =>
        assert(customerID == "AC001")
        assert(streetNumber == "123")
        assert(streetName == "Winter St")
        assert(city == "Wall")
        assert(country == "Winterfell")
    }
  }

  test("customerDocumentModel") {
    val accounts = Array("ACC001", "ACC002")
    val customerDocument = customerDocumentModel("12345", "John", "Snow", accounts, "123", "Winter St", "Wall", "Winterfell")

    val unapplyResult = customerDocumentModel.unapply(customerDocument)
    unapplyResult match {
      case Some((customerID, forename, surname, accounts,streetNumber, streetName, city, country)) =>
    assert(customerID == "12345")
    assert(forename == "John")
    assert(surname == "Snow")
    assert(accounts.sameElements(accounts))
    assert(streetNumber == "123" )
    assert(streetName == "Winter St")
    assert(city == "Wall")
    assert(country == "Winterfell")
  }
  }
}
