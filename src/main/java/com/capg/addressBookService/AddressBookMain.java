package com.capg.addressBookService;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.capg.addressBookService.AddressBookService.IOService;
import com.opencsv.exceptions.CsvValidationException;

	
	public class AddressBookMain{
		
		public enum IOService {
			CONSOLE_IO, FILE_IO, DB_IO, REST_IO
		};
		public static Map<String, AddressBook> addressBookMap;
		public static Map<String, Map<String, AddressBook>> stateBookMap;
		static Scanner scanner = new Scanner(System.in);

		public AddressBookMain() {
			addressBookMap = new HashMap<>();
			stateBookMap = new HashMap<>();
		}
		public void addData() {
			String option1, option2, option3;
			do {
				System.out.println("Enter the name of state");
				String stateForMap = scanner.nextLine();
				
				do {
					System.out.println("Enter the name of city");
					String cityForMap = scanner.nextLine();
					AddressBook addBook = new AddressBook(cityForMap);
					for (Map.Entry<String, AddressBook> entry : addressBookMap.entrySet()) {
						if (entry.getKey().equals(cityForMap)) {
							addBook = entry.getValue();
						}
					}
					addressBookMap.put(cityForMap, addBook);
					do {
						System.out.println("Enter the details of person");
						System.out.println("Enter the first name");
						String firstName = scanner.nextLine();
						System.out.println("Enter the last name");
						String lastName = scanner.nextLine();
						System.out.println("Enter the address");
						String address = scanner.nextLine();
						System.out.println("Enter the city name");
						String city = scanner.nextLine();
						System.out.println("Enter the state name");
						String state = scanner.next();
						System.out.println("Enter the ZIP code");
						long zip = scanner.nextLong();
						System.out.println("Enter the phone number");
						long phoneNumber = scanner.nextLong();
						scanner.nextLine();
						System.out.println("Enter the email");
						String email = scanner.nextLine();
						Contact c = new Contact(firstName, lastName, address, city, state, zip, phoneNumber, email);
						for (Map.Entry<String, AddressBook> entry : addressBookMap.entrySet()) {
							if (entry.getKey().equalsIgnoreCase(city)) {
								entry.getValue().addContact(c);
							}
						}
						System.out.println("Do you want to add contact again?");
						option1 = scanner.nextLine();
					} while (option1.equals("yes"));
					System.out.println("Do you want to add another city");
					option2 = scanner.nextLine();
				} while (option2.equals("yes"));
				stateBookMap.put(stateForMap, addressBookMap);
				System.out.println("Do you want to add for another state");
				option3 = scanner.nextLine();
			} while (option3.equals("yes"));
		}

		public void searchPersonByCity(String name, String city) {
			List<Contact> list = new ArrayList<Contact>();
			for(Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
				List<Contact> tempList = entry.getValue().getBook().stream()
						.filter(c-> c.getCity().equalsIgnoreCase(city))
						.filter(c->(c.getFirstName()+" "+c.getLastName())
						.equals(name)).collect(Collectors.toList());
				list.addAll(tempList); // Adding the temporary list data from streams to List
			}
			for(Contact c : list) {
				System.out.println(c);
			}
		}	
		public void searchPersonByState(String name, String state) {
			List<Contact> list = new ArrayList<Contact>();
			for(Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
				List<Contact> tempList = entry.getValue().getBook().stream()
						.filter(c-> c.getState().equalsIgnoreCase(state))
						.filter(c->(c.getFirstName()+" "+c.getLastName())
						.equalsIgnoreCase(name)).collect(Collectors.toList());
				list.addAll(tempList);
			}
			for(Contact c : list) {
				System.out.println(c);
			}
		}
		public void viewDataByCity(String city) {
			List<Contact> list = new ArrayList<Contact>();
			for(Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
				List<Contact> tempList = entry.getValue().getBook().stream()
						.filter(c-> c.getCity().equalsIgnoreCase(city))
						.collect(Collectors.toList());
				list.addAll(tempList);
			}
			for(Contact c : list) {
				System.out.println(c);
			}
		}
		public void viewDataByState(String state) {
			System.out.println("The List for state "+ state +" is :");
			for (Map.Entry<String,Map<String, AddressBook>> entry : stateBookMap.entrySet()) {
				if(entry.getKey().equalsIgnoreCase(state)) {
					for(Map.Entry<String,AddressBook> iEntry : addressBookMap.entrySet()) {
						System.out.println("The List for city "+ iEntry.getKey()+" is :");
						iEntry.getValue().viewList();
					}
				}
			}
		}
		public void countByCity(String city) {
			long count = 0;
			for(Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
				count = count + entry.getValue().getBook().stream()
						.filter(c-> c.getCity().equalsIgnoreCase(city))
						.count();
			}
			System.out.println("count is "+count);
		}
		public void countByState(String state) {
			long count = 0;
			for(Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
				count = count + entry.getValue().getBook().stream()
						.filter(c-> c.getState().equalsIgnoreCase(state))
						.count();
			}
			System.out.println("count is "+count);
		}
		public void sortByName() {
			for(Map.Entry<String, AddressBook> entry : addressBookMap.entrySet()) {
				Collections.sort(entry.getValue().getBook(),new SortByName());
			}

		}
		public void sortByZip() {
			for(Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
				Collections.sort(entry.getValue().getBook(),new SortByZip());
			}
		}

		/**
		 * Usecase13 Writing the data to file
		 * 
		 * @param ioService
		 */
		public void writeData(IOService ioService) throws AddressBookException {
			if (ioService.equals(IOService.FILE_IO)) {
				new AddressBookService().writeData(addressBookMap);
			}
		}

		/**
		 * Usecase13 Reading the data from file
		 * 
		 * @param ioService
		 */
		public void readData(IOService ioService) throws AddressBookException {
			if (ioService.equals(IOService.FILE_IO)) {
				new AddressBookService().readData();
			}
		}
		public static void main(String[] args) throws AddressBookException, CsvValidationException  {
			Scanner scanner = new Scanner(System.in);
			AddressBookMain addBookMain = new AddressBookMain();
			int v;
			while(true) {
				
				System.out.println("1.to add person\n2.to edit contact\n3.to delete contact\n4.to view addbook\n5.to search contact in city\n6.to search contact in state\n7.to view data by city\n8.to view data in state\n9.to count contact from city"
						+ "\n10.to count contact from state\n11.to sort the addressbook by name\n12.to sort the addressbook by zip\n13.Writing data to file\n14.Reading data from File\n15.exit");
				v = scanner.nextInt();
				scanner.nextLine();
				switch(v) {
					case 1:  
						addBookMain.addData();
						break;
					case 2:
						System.out.println("Enter the name to edit contact");
						String x = scanner.nextLine();
						System.out.println("Enter the city");
						String city2 = scanner.nextLine();
						for (Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {   
							if(entry.getKey().equalsIgnoreCase(city2)) {
								entry.getValue().editContact(x);
							}
							else {
								System.out.println("The addressbook does not exist, please create addressbook for that city");
							}
						}
						break;
					case 3:
						System.out.println("Enter the name to delete");
						String y = scanner.nextLine();
						System.out.println("Enter the city");
						String city1 = scanner.nextLine();
						for (Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {   
							if(entry.getKey().equalsIgnoreCase(city1)) {
								entry.getValue().deleteContact(y);
							}
							else {
								System.out.println("The addressbook does not exist, please create addressbook for that city");
							}
						}
						break;
					case 4:
						for (Map.Entry<String,AddressBook> entry : addressBookMap.entrySet()) {
							System.out.println("The addressbook for city "+entry.getKey()+" is :");
							entry.getValue().viewList();
						}
						break;
					case 5:
						System.out.println("Enter the name to search");
						String person = scanner.nextLine();
						System.out.println("Enter the city");
						String citi = scanner.nextLine();
						addBookMain.searchPersonByCity(person,citi);
						break;
					case 6:
						System.out.println("Enter the name to search");
						String per = scanner.nextLine();
						System.out.println("Enter the state");
						String stat = scanner.nextLine();
						addBookMain.searchPersonByCity(per,stat);
						break;
					case 7:
						System.out.println("Enter the city");
						String citii = scanner.nextLine();
						addBookMain.viewDataByCity(citii);
						break;
					case 8:
						System.out.println("Enter the state");
						String stats = scanner.nextLine();
						addBookMain.viewDataByState(stats);
						break;
					case 9:
						System.out.println("Enter the city");
						String citi1 = scanner.nextLine();
						addBookMain.countByCity(citi1);
						break;
					case 10:
						System.out.println("Enter the city");
						String stat1 = scanner.nextLine();
						addBookMain.countByState(stat1);
						break;
					case 11:
						addBookMain.sortByName();
						break;
					case 12:
						addBookMain.sortByZip();
						break;
					case 13:
						addBookMain.writeData(IOService.FILE_IO);
						break;
					case 14:
						addBookMain.readData(IOService.FILE_IO);
						break;
					case 15:
						new AddressBookService().writeDataToCSV(addressBookMap);
						new AddressBookService().readDataFromCSV();
						break;
					case 16:
						try {
							new AddressBookService().writeDataToJSON(addressBookMap);
							new AddressBookService().readDataFromJSON();
						} catch (IOException e) {
							e.printStackTrace();
						}
						break;
					case 17:
						System.exit(0);
				}
			}
		}
	}	
	