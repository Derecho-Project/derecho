package com.derecho.objectstore;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * @author xinzheyang a command line interface demo for ObjectStore API
 */
public class OSSTest {
	Map<String, String> commands;

	private OSSTest() {
		commands = new HashMap<>();
		commands.put("put", "put <oid> <string>");
		commands.put("get", "get <oid>");
		commands.put("remove", "remove <oid>");
		commands.put("leave", "leave");
	}

	private void help() {
		System.out.println("Commands:");
		for (String helpinfo : commands.values()) {
			System.out.println(helpinfo);
		}
	}

	public static void main(String[] args) {
		OSSTest ossTest = new OSSTest();
		

		ObjectStoreService oss = ObjectStoreService.getObjectStoreService("./test");
		
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		boolean bNextCommand = true;

		ossTest.help();
		while (bNextCommand) {
			System.out.print("cmd>");
			String line = scanner.nextLine().trim();
			String command = line;
			int first_space=0;
			if (line.contains(" ")) {
				first_space = line.indexOf(" ");
				command = line.substring(0, first_space);
			}

			if (ossTest.commands.containsKey(command)) {
				String[] arguments = line.substring(first_space + 1).trim().split("\\s+");
				boolean success = false;
				if (command.equals("put")) {
					try {
						oss.put(Long.parseLong(arguments[0]), arguments[1]);
						success = true;
					} catch (Exception e) {
					}
				} else if (command.equals("get")) {
					try {
						System.out.println(oss.get(Long.parseLong(arguments[0])));
						success = true;
					} catch (Exception e) {
					}
				} else if (command.equals("remove")) {
					try {
						success = oss.remove(Long.parseLong(arguments[0]));
					} catch (Exception e) {
					}
				} else if (command.equals("leave")) {
					try {
						oss.leave();
						success = true;
					} catch (Exception e) {
					}
				}
				System.out.print("Success: ");
				System.out.println(success);

			} else {
				ossTest.help();
			}
		}
	}
}

