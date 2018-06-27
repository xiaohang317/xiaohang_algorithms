package drpc;

/***
 *用户的服务
 */

public interface UserService {

    public static final long versionID = 88888888;
    /***
     *
     * @param name
     * @param age
     */
    void addUser(String name,int age);
}
