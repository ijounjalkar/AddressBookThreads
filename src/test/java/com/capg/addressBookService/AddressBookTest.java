package com.capg.addressBookService;

import static org.junit.Assert.assertEquals;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import org.junit.Test;


import com.capg.addressBookService.AddressBookMain.IOService;
import com.capg.addressBookService.AddressBookService;
import com.capg.addressBookService.Contact;
import com.capg.addressBookService.DatabaseException;



public class AddressBookTest {
	/**
	 * Usecase16: Retrieve data from the database
	 * 
	 * @throws DatabaseException
	 * @throws SQLException 
	 */
	@Test
	public void givenContactDataInDB_WhenRetrieved_ShouldMatchContactCount() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		List<Contact> contactData = addressBookService.readContactData(IOService.DB_IO);
		assertEquals(3, contactData.size());
	}
	
	/**Usecase17: Updating phone number of a persons in contact table
	 * @throws DatabaseException
	 * @throws SQLException
	 */
	@Test
	public void givenNewDataForContact_WhenUpdated_ShouldBeInSync() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		List<Contact> contactData = addressBookService.readContactData(IOService.DB_IO);
		addressBookService.updatePersonsPhone("Isha", "7457120752");
		addressBookService.readContactData(IOService.DB_IO);
		boolean result = addressBookService.checkContactDataSync("Isha");
		assertEquals(true, result);
	}
	
	/**
	 * Usecase18: retrieving data from the table between data range
	 * 
	 * @throws DatabaseException
	 * @throws SQLException 
	 */
	@Test
	public void givenContactInDB_WhenRetrievedForDateRange_ShouldMatchContactCount() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		List<Contact> contactData = addressBookService.readContactData(IOService.DB_IO);
		List<Contact> resultList = addressBookService.getContactForDateRange(LocalDate.of(2020, 01, 01),
				LocalDate.of(2021, 01, 01));
		assertEquals(1, resultList.size());
	}
	/**
	 * Usecase19: retrieving data from the table for city and state
	 * 
	 * @throws DatabaseException
	 * @throws SQLException 
	 */
	@Test
	public void givenContactInDB_WhenRetrievedForCityAndState_ShouldMatchContactCount() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		List<Contact> contactData = addressBookService.readContactData(IOService.DB_IO);
		List<Contact> resultList = addressBookService.getContactForCityAndState("Akola", "Maharashta");
		assertEquals(2, resultList.size());
	}

	/**
	 * Usecase20: Insert data into database in a single transaction
	 * 
	 * @throws DatabaseException
	 * @throws SQLException
	 */
	@Test
	public void givenContactInDB_WhenAdded_ShouldBeAddedInSingleTransaction() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		List<Contact> contactData = addressBookService.readContactData(IOService.DB_IO);
		addressBookService.addContactInDatabase("Leena", "Sarode", "Panvel", "400019", "Mumbai", "Maharashtra",
                                               "9859629542", "hjhgy@gmail.com", LocalDate.of(2021, 01, 01), "AddressBook1", "friend");
		boolean result = addressBookService.checkContactDataSync("Leena");
		assertEquals(true, result);
	}
	
	
}