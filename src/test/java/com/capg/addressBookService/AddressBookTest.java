package com.capg.addressBookService;

import static org.junit.Assert.assertEquals;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;


import com.capg.addressBookService.AddressBookMain.IOService;
import com.capg.addressBookService.AddressBookService;
import com.capg.addressBookService.Contact;
import com.capg.addressBookService.DatabaseException;
import com.google.gson.Gson;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;



public class AddressBookTest {
	@Test
	public void givenContactDataInDB_WhenRetrieved_ShouldMatchContactCount() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		List<Contact> contactData = addressBookService.readContactData(IOService.DB_IO);
		assertEquals(4, contactData.size());
	}

	@Test
	public void givenNewDataForContact_WhenUpdated_ShouldBeInSync() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		addressBookService.readContactData(IOService.DB_IO);
		addressBookService.updatePersonsPhone("Isha Jounjalkar", 8850273350L, null);
		addressBookService.readContactData(IOService.DB_IO);
		boolean result = addressBookService.checkContactDataSync("Isha Jounjalkar");
		assertEquals(true, result);
	}

	
	@Test
	public void givenContactInDB_WhenRetrievedForDateRange_ShouldMatchContactCount() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		addressBookService.readContactData(IOService.DB_IO);
		List<Contact> resultList = addressBookService.getContactForDateRange(LocalDate.of(2020, 01, 01),
				LocalDate.of(2021, 01, 01));
		assertEquals(1, resultList.size());
	}

	
	@Test
	public void givenContactInDB_WhenRetrievedForCityAndState_ShouldMatchContactCount() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		addressBookService.readContactData(IOService.DB_IO);
		List<Contact> resultList = addressBookService.getContactForCityAndState("Nagpur", "Maharashta");
		assertEquals(2, resultList.size());
	}

	
	@Test
	public void givenContactInDB_WhenAdded_ShouldBeAddedInSingleTransaction() throws DatabaseException, SQLException {
		AddressBookService addressBookService = new AddressBookService();
		addressBookService.readContactData(IOService.DB_IO);
		addressBookService.addContactInDatabase("Leena","Sarode", "Panvel", 400019, "Mumbai", "Maharashtra",
				985962954, "hjhgy@gmail.com", LocalDate.of(2021, 01, 01), 1, null, null);
		boolean result = addressBookService.checkContactDataSync("Leena");
		assertEquals(true, result);
	}
	@Test
	public void geiven2Contacts_WhenAddedToDB_ShouldMatchContactEntries() throws DatabaseException, SQLException {
		Contact[] contactArray = { new Contact("Leena","Sarode","Panvel", "Mumbai","Maharashtra", 400019, 985962954,"hjhgy@gmail.com",LocalDate.of(2021, 01, 01),2),
				new Contact("Tushar","Patil","Shivaji Nagar", "Kolhapur","Maharashtra", 410554, 952269856,"hjhvsgks@gmail.com",LocalDate.of(2021, 01, 01),2)};
		AddressBookService addressBookService = new AddressBookService();
		addressBookService.readContactData(IOService.DB_IO);
		Instant start = Instant.now();
		addressBookService.addContactToDB(Arrays.asList(contactArray));
		Instant threadEnd = Instant.now();
		System.out.println("Duration with Thread: " + Duration.between(start, threadEnd));
		long result = addressBookService.countEntries(IOService.DB_IO);
		assertEquals(3, result);
	}
	@Test
	public void geiven2Persons_WhenUpdatedPhoneNumer_ShouldSyncWithDB() throws DatabaseException, SQLException {
		Map<String, Long> contactMap = new HashMap<>();
		contactMap.put("Leena Sarode",(long) 985962954);
		contactMap.put("Tushar Patil",(long) 952269856);
		AddressBookService addressBookService = new AddressBookService();
		addressBookService.readContactData(IOService.DB_IO);
		Instant start = Instant.now();
		addressBookService.updatePhoneNumber(contactMap);
		Instant end = Instant.now();
		System.out.println("Duration with Thread: " + Duration.between(start, end));
		boolean result = addressBookService.checkContactInSyncWithDB(Arrays.asList("Leena Sarode"));
		assertEquals(true,result);
	}
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost";
		RestAssured.port = 3000;
	}
	
	private Contact[] getContactList() {
		Response response = RestAssured.get("/contact");
		System.out.println("Contact entries in JSONServer:\n"+response.asString());
		Contact[] arrayOfContact = new Gson().fromJson(response.asString(),Contact[].class);
		return arrayOfContact;
	}
	private Response addContactToJsonServer(Contact contact) {
		String contactJson = new Gson().toJson(contact);
		RequestSpecification request = RestAssured.given();
		request.header("Content-Type", "application/json");
		request.body(contactJson);
		return request.post("/contact");
	}
	
	@Test
	public void givenContactDataInJSONServer_WhenRetrieved_ShouldMatchTheCount() {
		Contact[] arrayOfContact = getContactList();
		AddressBookService addressBookService = new AddressBookService(Arrays.asList(arrayOfContact));
		long entries = addressBookService.countEntries(IOService.REST_IO);
		assertEquals(1,entries);
	}
	
	@Test
	public void givenListOfNewContacts_WhenAdded_ShouldMatch201ResponseAndCount() {
		Contact[] arrayOfContact = getContactList();
		AddressBookService addService = new AddressBookService(Arrays.asList(arrayOfContact));
		Contact[] arrayOfCon = {new Contact("Leena","Sarode","Panvel", "Mumbai","Maharashtra", 400019, 985962954,"hjhgy@gmail.com",LocalDate.of(2021, 01, 01),2),
				new Contact("Tushar","Patil","Shivaji Nagar", "Kolhapur","Maharashtra", 410554, 952269856,"hjhvsgks@gmail.com",LocalDate.of(2021, 01, 01),2)};
		List<Contact> contactList = Arrays.asList(arrayOfCon);
		contactList.forEach(contact -> {
			Runnable task = () -> {
				Response response = addContactToJsonServer(contact);
				int statusCode = response.getStatusCode();
				assertEquals(201, statusCode);
				Contact newContact = new Gson().fromJson(response.asString(), Contact.class);
				addService.addContactToAddressBook(newContact);
			};
			Thread thread = new Thread(task, contact.firstName);
			thread.start();
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		long count = addService.countEntries(IOService.REST_IO);
		assertEquals(3, count);
	}
	@Test 
	public void givenNewPhoneForContact_WhenUpdated_ShouldMatch200Request() throws DatabaseException, SQLException {
		Contact[] arrayOfContact = getContactList();
		AddressBookService addService = new AddressBookService(Arrays.asList(arrayOfContact));
		addService.updatePersonsPhone("Tushar",952269856,IOService.REST_IO);
		Contact contact = addService.getContact("Tushar");
		String contactJson = new Gson().toJson(contact);
		RequestSpecification request = RestAssured.given();
		request.header("Content-Type","application/json");
		request.body(contactJson);
		Response response = request.put("/contact/"+contact.id);
		int statusCode = response.getStatusCode();
		assertEquals(200,statusCode);			
	}
	@Test 
	public void givenContactToDelete_WhenDeleted_ShouldMatch200ResponseAndCount() throws DatabaseException, SQLException {
		Contact[] arrayOfContact = getContactList();
		AddressBookService addService = new AddressBookService(Arrays.asList(arrayOfContact));
		Contact contact = addService.getContact("Tushar");
		RequestSpecification request = RestAssured.given();
		request.header("Content-Type","application/json");
		Response response = request.delete("/contact/"+contact.id);
		int statusCode = response.getStatusCode();
		assertEquals(200,statusCode);
		addService.deleteContactFromAddressBook(contact.firstName, IOService.REST_IO);
		long count = addService.countEntries(IOService.REST_IO);
		assertEquals(1,count);
	}

