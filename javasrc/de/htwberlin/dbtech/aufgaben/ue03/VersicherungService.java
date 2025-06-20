package de.htwberlin.dbtech.aufgaben.ue03;

/*
  @author Ingo Classen
 */

import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * VersicherungJdbc
 */
public class VersicherungService implements IVersicherungService {
    private static final Logger L = LoggerFactory.getLogger(VersicherungService.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("unused")
    private Connection useConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {

        //check1
        if(!isVertragExisting(vertragsId)) {
            throw new VertragExistiertNichtException(vertragsId);
        }
        //check2
        if (!isDeckungsartExisting(deckungsartId)) {
            throw new DeckungsartExistiertNichtException(deckungsartId);
        }
        //check3
        if (!passtDeckungsartZuVertrag(vertragsId, deckungsartId)) {
            throw new DeckungsartPasstNichtZuProduktException(vertragsId, deckungsartId);
        }
        //check4
        if (!isDeckungsbetragGueltig(deckungsartId, deckungsbetrag)) {
            throw new UngueltigerDeckungsbetragException(deckungsbetrag);
        }
        //check5
        if (!isPreisVorhanden(deckungsartId, deckungsbetrag)) {
            throw new DeckungspreisNichtVorhandenException(deckungsbetrag);
        }
        //check6
        if (!isDeckungRegelkonform(vertragsId, deckungsartId, deckungsbetrag)) {
            throw new DeckungsartNichtRegelkonformException(deckungsartId);
        }

        /**
         * Insert the coverage into the database.
         *
         * @param vertragsId     The ID of the contract.
         * @param deckungsartId  The ID of the coverage type.
         * @param deckungsbetrag The coverage amount.
         */
        String sql = """
    INSERT INTO deckung (vertrag_fk, deckungsart_fk, deckungsbetrag)
    VALUES (?, ?, ?)
""";

        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, vertragsId);
            ps.setInt(2, deckungsartId);
            ps.setBigDecimal(3, deckungsbetrag);
            ps.executeUpdate();
            L.info("Deckung erfolgreich gespeichert.");
        } catch (Exception ex) {
            throw new DataException("Fehler beim Einfügen der Deckung", ex);
        }
    }

    /**
     * Checks if a contract with the given ID exists in the database.
     *
     * @param vertragsId The ID of the contract to check.
     * @return true if the contract exists, false otherwise.
     */
    public boolean isVertragExisting(Integer vertragsId) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "SELECT COUNT(ID)  AS ANZAHL FROM vertrag WHERE id = ?";

        try{
            ps = useConnection().prepareStatement(sql);
            ps.setInt(1, vertragsId);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt("Anzahl") > 0;
            } else
                return false;
        } catch (Exception ex) {
            throw new DataException(ex);
        }

    }

    /**
     * Checks if a coverage type with the given ID exists in the database.
     *
     * @param deckungsartId The ID of the coverage type to check.
     * @return true if the coverage type exists, false otherwise.
     */
    public boolean isDeckungsartExisting(Integer deckungsartId) {
        String sql = "SELECT COUNT(id) FROM deckungsart WHERE id = ?";
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }

    /**
     * Checks if the coverage type matches the product of the contract.
     *
     * @param vertragsId    The ID of the contract.
     * @param deckungsartId The ID of the coverage type.
     * @return true if the coverage type matches the product, false otherwise.
     */
    public boolean passtDeckungsartZuVertrag(Integer vertragsId, Integer deckungsartId) {
        String sql = """
        SELECT COUNT(*) 
        FROM vertrag v
        JOIN deckungsart d ON v.produkt_fk = d.produkt_fk
        WHERE v.id = ? AND d.id = ?
        """;
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, vertragsId);
            ps.setInt(2, deckungsartId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }

    /**
     * Checks if the coverage amount is valid for the given coverage type.
     *
     * @param deckungsartId The ID of the coverage type.
     * @param deckungsbetrag The coverage amount to check.
     * @return true if the coverage amount is valid, false otherwise.
     */
    public boolean isDeckungsbetragGueltig(Integer deckungsartId, BigDecimal deckungsbetrag) {
        String sql = """
        SELECT COUNT(*) 
        FROM deckungsbetrag 
        WHERE deckungsart_fk = ? AND deckungsbetrag = ?
        """;
        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            ps.setBigDecimal(2, deckungsbetrag);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }

    /**
     * Checks if a price exists for the given coverage type and amount.
     *
     * @param deckungsartId The ID of the coverage type.
     * @param deckungsbetrag The coverage amount to check.
     * @return true if a price exists, false otherwise.
     */
    public boolean isPreisVorhanden(Integer deckungsartId, BigDecimal deckungsbetrag) {
        String sql = """
        SELECT COUNT(*) 
        FROM deckungspreis p
        JOIN deckungsbetrag db ON p.deckungsbetrag_fk = db.id
        WHERE db.deckungsart_fk = ? 
          AND db.deckungsbetrag = ? 
          AND p.gueltig_bis >= SYSDATE
        """;

        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            ps.setBigDecimal(2, deckungsbetrag);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }

    /**
     * Checks if the coverage is compliant with the rejection rules.
     *
     * @param vertragsId     The ID of the contract.
     * @param deckungsartId  The ID of the coverage type.
     * @param deckungsbetrag The coverage amount to check.
     * @return true if the coverage is compliant, false otherwise.
     */
    public boolean isDeckungRegelkonform(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        String sql = """
        SELECT r.r_betrag, r.r_alter, k.geburtsdatum
        FROM vertrag v
        JOIN kunde k ON v.kunde_fk = k.id
        JOIN ablehnungsregel r ON r.deckungsart_fk = ?
        WHERE v.id = ?
        """;

        try (PreparedStatement ps = useConnection().prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            ps.setInt(2, vertragsId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String rBetrag = rs.getString("r_betrag");
                    String rAlter = rs.getString("r_alter");
                    java.sql.Date geburtsdatum = rs.getDate("geburtsdatum");

                    int alter = calculateAge(geburtsdatum.toLocalDate());

                    boolean betragErfuellt = evaluateCondition(rBetrag, deckungsbetrag);
                    boolean alterErfuellt = evaluateCondition(rAlter, new BigDecimal(alter));

                    if (betragErfuellt && alterErfuellt) {
                        return false; // Regel nicht konform
                    }
                }
                return true; // alle Regeln OK
            }

        } catch (Exception ex) {
            throw new DataException(ex);
        }
    }



    //Hilfsmethoden

    /**
     * Berechnet das Alter basierend auf dem Geburtsdatum.
     *
     * @param geburtsdatum Das Geburtsdatum des Kunden.
     * @return Das Alter in Jahren.
     */
    public int calculateAge(java.time.LocalDate geburtsdatum) {
        return java.time.Period.between(geburtsdatum, java.time.LocalDate.now()).getYears();
    }

    /**
     * Evaluates a condition against a value.
     *
     * @param condition The condition to evaluate (e.g., ">=", "<", "!=").
     * @param value     The value to compare against the condition.
     * @return true if the condition is met, false otherwise.
     */
    private boolean evaluateCondition(String condition, BigDecimal value) {
        if (condition == null || condition.trim().equals("--") || condition.trim().isEmpty()) {
            return true; // Keine Bedingung vorhanden ⇒ immer OK
        }

        condition = condition.trim();

        // Operator extrahieren (z. B. ">=", "<", "!=" usw.)
        String operator = condition.replaceAll("[0-9]", "").trim();
        String zahlTeil = condition.replaceAll("[^0-9]", "").trim();

        // Wenn keine Zahl vorhanden → Fehler vermeiden
        if (zahlTeil.isEmpty()) {
            return true; // Lieber OK statt crashen
        }

        BigDecimal vergleichswert = new BigDecimal(zahlTeil);

        return switch (operator) {
            case "=" -> value.compareTo(vergleichswert) == 0;
            case "!=" -> value.compareTo(vergleichswert) != 0;
            case "<" -> value.compareTo(vergleichswert) < 0;
            case "<=" -> value.compareTo(vergleichswert) <= 0;
            case ">" -> value.compareTo(vergleichswert) > 0;
            case ">=" -> value.compareTo(vergleichswert) >= 0;
            default -> true; // Unbekannter Operator = kein Blocker
        };
    }
}