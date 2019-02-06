/**
 * @author xinzheyang
 *
 */
public class OSSTest {



	public static void main(String[] args) {
		ObjectStoreService objectStoreService = ObjectStoreService.getObjectStoreService("./test");
		objectStoreService.put("124", "helloo");
		objectStoreService.put("43", "hiiiii");
		System.out.println(objectStoreService.get("124"));
		System.out.println(objectStoreService.get("43"));
		System.out.println(objectStoreService.remove("43"));
		System.out.println(objectStoreService.get("43"));
		objectStoreService.leave();
	}

}
