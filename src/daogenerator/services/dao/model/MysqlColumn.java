package daogenerator.services.dao.model;
/**
 * Define MysqlColumn as a structure of the returned table columns in Mysql.
 * @author wenqili
 *
 */
public class MysqlColumn {
	String Field;
	String Type;
	String Null;
	String Key;
	String Default;
	String Extra;

	public String getField() {
		return Field;
	}

	public void setField(String field) {
		Field = field;
	}

	public String getType() {
		return Type;
	}

	public void setType(String type) {
		Type = type;
	}

	public String getNull() {
		return Null;
	}

	public void setNull(String null1) {
		Null = null1;
	}

	public String getKey() {
		return Key;
	}

	public void setKey(String key) {
		Key = key;
	}

	public String getDefault() {
		return Default;
	}

	public void setDefault(String default1) {
		Default = default1;
	}

	public String getExtra() {
		return Extra;
	}

	public void setExtra(String extra) {
		Extra = extra;
	}

}
