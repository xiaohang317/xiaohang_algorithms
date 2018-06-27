package drpc;

/***
 *
 */
public class UserServiceImpl implements UserService{
    public void addUser(String name, int age) {
        System.out.println("From Server invoked: add user success...name is :"+name);
    }
}
