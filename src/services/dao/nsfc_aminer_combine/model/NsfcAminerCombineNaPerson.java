package services.dao.nsfc_aminer_combine.model;


public class NsfcAminerCombineNaPerson {
	Integer id;
	String names;
	Integer contact_id;
	Short type;
	public Integer getId() {
		return this.id;
	}
	public String getNames() {
		return this.names;
	}
	public Integer getContactId() {
		return this.contact_id;
	}
	public Short getType() {
		return this.type;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public void setNames(String names) {
		this.names = names;
	}
	public void setContactId(Integer contact_id) {
		this.contact_id = contact_id;
	}
	public void setType(Short type) {
		this.type = type;
	}
}
