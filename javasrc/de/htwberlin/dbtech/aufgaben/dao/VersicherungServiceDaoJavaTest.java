package de.htwberlin.dbtech.aufgaben.dao;

import de.htwberlin.dbtech.exceptions.*;
import de.htwberlin.dbtech.utils.DbCred;
import de.htwberlin.dbtech.utils.DbUnitUtils;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.*;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.aufgaben.ue03.IVersicherungService;


import java.io.File;
import java.math.BigDecimal;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VersicherungServiceDaoJavaTest {

    private static final Logger L = LoggerFactory.getLogger(VersicherungServiceDaoJavaTest.class);
    private static IDatabaseConnection dbTesterCon = null;

    private static IVersicherungService vService;

    @BeforeClass
    public static void setUp() {
        L.debug("setUp: start");
        try {
            IDatabaseTester dbTester = new JdbcDatabaseTester(DbCred.driverClass, DbCred.url, DbCred.user, DbCred.password,
                    DbCred.schema);
            dbTesterCon = dbTester.getConnection();
            IDataSet datadir = new CsvDataSet(new File("test-data/ue03-04"));
            dbTester.setDataSet(datadir);
            DatabaseOperation.CLEAN_INSERT.execute(dbTesterCon, datadir);

            // HIER: DAO statt bisheriger Service
            vService = new VersicherungServiceDao(dbTesterCon.getConnection());

        } catch (Exception e) {
            DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() {
        L.debug("tearDown: start");
        DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
    }

    @Test(expected = VertragExistiertNichtException.class)
    public void createDeckung01() {
        vService.createDeckung(99, 1, BigDecimal.valueOf(0));
    }

    @Test(expected = DeckungsartExistiertNichtException.class)
    public void createDeckung02() {
        vService.createDeckung(5, 99, BigDecimal.valueOf(0));
    }

    @Test(expected = DeckungsartPasstNichtZuProduktException.class)
    public void createDeckung03() {
        vService.createDeckung(5, 1, BigDecimal.valueOf(0));
    }

    @Test(expected = UngueltigerDeckungsbetragException.class)
    public void createDeckung04() {
        vService.createDeckung(5, 6, BigDecimal.valueOf(0));
    }

    @Test(expected = UngueltigerDeckungsbetragException.class)
    public void createDeckung05() {
        vService.createDeckung(5, 5, BigDecimal.valueOf(2000));
    }

    @Test(expected = DeckungspreisNichtVorhandenException.class)
    public void createDeckung06() {
        vService.createDeckung(5, 5, BigDecimal.valueOf(1500));
    }

    @Test(expected = DeckungspreisNichtVorhandenException.class)
    public void createDeckung07() {
        vService.createDeckung(5, 4, BigDecimal.valueOf(150000));
    }

    @Test(expected = DeckungsartNichtRegelkonformException.class)
    public void createDeckung08() {
        vService.createDeckung(6, 1, BigDecimal.valueOf(100000000));
    }

    @Test(expected = DeckungsartNichtRegelkonformException.class)
    public void createDeckung09() {
        vService.createDeckung(7, 3, BigDecimal.valueOf(100000));
    }

    @Test(expected = DeckungsartNichtRegelkonformException.class)
    public void createDeckung10() {
        vService.createDeckung(8, 3, BigDecimal.valueOf(200000));
    }

    @Test(expected = DeckungsartNichtRegelkonformException.class)
    public void createDeckung11() {
        vService.createDeckung(9, 3, BigDecimal.valueOf(300000));
    }

    @Test
    public void createDeckung12() throws Exception {
        Integer[] vertragsIds = new Integer[]{5, 8, 9};
        Integer[] deckungsartIds = new Integer[]{4, 3, 3};
        BigDecimal[] deckungsbetraege = new BigDecimal[]{
                BigDecimal.valueOf(50000),
                BigDecimal.valueOf(100000),
                BigDecimal.valueOf(200000)
        };
        for (int i = 0; i < 3; i++) {
            vService.createDeckung(vertragsIds[i], deckungsartIds[i], deckungsbetraege[i]);
        }

        QueryDataSet databaseDataSet = new QueryDataSet(dbTesterCon);
        String sql = "select * from Deckung where Vertrag_FK in (5, 8, 9) order by Vertrag_FK, Deckungsart_FK";
        databaseDataSet.addTable("Deckung", sql);
        ITable tblDeckung = databaseDataSet.getTable("Deckung");

        Assert.assertEquals("Falsche Anzahl Zeilen", 3, tblDeckung.getRowCount());

        for (int i = 0; i < 3; i++) {
            Integer vertragsId = ((BigDecimal) tblDeckung.getValue(i, "Vertrag_FK")).intValue();
            Integer deckungsartId = ((BigDecimal) tblDeckung.getValue(i, "Deckungsart_FK")).intValue();
            BigDecimal deckungsbetrag = (BigDecimal) tblDeckung.getValue(i, "Deckungsbetrag");
            Assert.assertEquals("Falsche vertragsId", vertragsIds[i], vertragsId);
            Assert.assertEquals("Falsche deckungsartId", deckungsartIds[i], deckungsartId);
            Assert.assertEquals("Falscher deckungsbetrag", deckungsbetraege[i], deckungsbetrag);
        }
    }
}
