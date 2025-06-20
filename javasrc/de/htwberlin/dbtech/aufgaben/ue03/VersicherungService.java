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

}