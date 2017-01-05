package com.DB.kolya;

import com.lits.kundera.test.BaseTest;
import org.junit.Assert;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.io.IOException;
import java.util.List;

public class CustomTest extends BaseTest{

    @Override
    public void customTest() throws IOException {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu");
        EntityManager em = emf.createEntityManager();

        Query findQuery = em.createQuery("select p from Physician p");
        List<com.DB.kolya.Physician> allPhysicians = findQuery.getResultList();
        Assert.assertNotEquals(0, allPhysicians.size());

        Query findQuery2 = em.createQuery("select p from Patient p where p.firstName = \'Patient1\'");
        List<com.DB.kolya.Patient> allJohns = findQuery2.getResultList();
        Assert.assertEquals(1, allJohns.size());

        Query findQuery3 = em.createQuery("select mr from MedicalRecord mr where mr.type = \'type2\'");
        List<com.DB.kolya.MedicalRecord> allRecords = findQuery3.getResultList();
        Assert.assertEquals(1, allRecords.size());
    }

}
