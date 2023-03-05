/**
 * @author 张翔
 * @date 2023/3/2 11:20
 * @description
 */
public class TestBean {
    private String id;
    private String name;
    private String addr;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public TestBean(String id, String name, String addr) {
        this.id = id;
        this.name = name;
        this.addr = addr;
    }

    public TestBean() {
    }

    @Override
    public String toString() {
        return "TestBean{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", addr='" + addr + '\'' +
                '}';
    }
}
