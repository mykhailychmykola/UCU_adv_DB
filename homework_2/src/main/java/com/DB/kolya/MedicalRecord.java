package com.DB.kolya;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Table(name = "medical_records", schema = "default@hbase_pu")
public class MedicalRecord {
    public MedicalRecord(){}

    @Id
    @Column(name="id")
    private byte[] id;

    @Column(name="type")
    private String type;

    @Column(name = "description")
    private String description;

    @Column(name = "date_performed")
    private Date datePerformed;

    public byte[] getId() {
        return id;
    }

    public void setId(byte[] id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getDatePerformed() {
        return datePerformed;
    }

    public void setDatePerformed(Date datePerformed) {
        this.datePerformed = datePerformed;
    }
}
