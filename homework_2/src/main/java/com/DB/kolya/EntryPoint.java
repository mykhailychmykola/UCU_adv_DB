package com.DB.kolya;

import com.google.common.primitives.Bytes;
import org.fluttercode.datafactory.impl.DataFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class EntryPoint {
    public static void main(String[] args) throws IOException {
        DataFactory df = new DataFactory();
        List<String> types = new ArrayList<>(Arrays.asList("type1", "type2", "type3", "type4"));

        Map<String, String> properties = new HashMap<>();
        EntityManagerFactory factory = Persistence.createEntityManagerFactory("hbase_pu", properties);
        EntityManager manager = factory.createEntityManager();


        Physician physician = new Physician();
        physician.setId(MyUtil.toBytes(UUID.randomUUID()));
        physician.setSpecialization("Proctolog");
        physician.setFullName("Proctolog Zhopoglad");
        physician.setClinicName("Clinic1");

        Patient patient;
        MedicalRecord medicalRecord;
        for (int i = 0; i < 100; i++) {
            patient = new Patient();
            patient.setFirstName("FirstName" + String.valueOf(i));
            patient.setLastName("LastName" + String.valueOf(i));
            patient.setDateOfBirth(df.getBirthDate());
            patient.setId(MyUtil.toBytes(UUID.randomUUID()));

            for (int k = 0; k < 3; k++) { // 3 medical records for each patient
                medicalRecord = new MedicalRecord();
                medicalRecord.setDatePerformed(df.getBirthDate());
                medicalRecord.setType(df.getItem(types));
                medicalRecord.setId(Bytes.concat(MyUtil.toBytes(UUID.randomUUID()), patient.getId()));
                medicalRecord.setDescription("Description " + String.valueOf(i) + String.valueOf(k));

                manager.persist(medicalRecord);
            }
            manager.persist(patient);
        }


        manager.persist(physician);
//
        manager.close();
        factory.close();
        CustomTest test = new CustomTest();
        try {
            test.runSuite();
        } catch (IOException ex) {

        }
    }


}

