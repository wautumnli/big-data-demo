package top.tzk.kafka;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/3
 * @Description:
 * @Modified By:
 */
public enum KafKaEnum {

    TOPIC("sensor","topic"),
    HOST("39.97.123.131:9092","host");

    private String value;
    private String name;

    KafKaEnum(String value, String name) {
        this.value = value;
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public String getName() {
        return name;
    }
}
