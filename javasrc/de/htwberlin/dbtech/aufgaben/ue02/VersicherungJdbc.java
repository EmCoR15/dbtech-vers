package de.htwberlin.dbtech.aufgaben.ue02;


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
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * VersicherungJdbc
 */
public class VersicherungJdbc implements IVersicherungJdbc {
    private static final Logger L = LoggerFactory.getLogger(VersicherungJdbc.class);
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
    public List<String> kurzBezProdukte() {

        List<String> kurzBez = new LinkedList<String>();
        String query = "SELECT kurzbez from produkt order by id asc";
        PreparedStatement ps = null;
        ResultSet rs = null;

        try{
            ps = useConnection().prepareStatement(query);

            rs = ps.executeQuery();
            while (rs.next()) {
                kurzBez.add(rs.getString("kurzbez"));
            }
        }
        catch (SQLException e) {
            throw new DataException(e);

        }

        return kurzBez;
    }

    @Override
    public Kunde findKundeById(Integer id) {
        String query = "SELECT id, name, geburtsdatum FROM kunde WHERE id = ?";
        try (PreparedStatement ps = useConnection().prepareStatement(query)) {
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                Integer kid = rs.getInt("id");
                String name = rs.getString("name");
                LocalDate gebDatum = rs.getDate("geburtsdatum").toLocalDate();
                return new Kunde(kid, name, gebDatum);
            } else {
                throw new KundeExistiertNichtException(id);
            }
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void createVertrag(Integer id, Integer produktId, Integer kundenId, LocalDate versicherungsbeginn) {
        if (versicherungsbeginn.isBefore(LocalDate.now())) {
            throw new DatumInVergangenheitException(versicherungsbeginn);
        }

        try {
            // PrÃ¼fe: Vertrag existiert bereits?
            PreparedStatement checkVertrag = useConnection().prepareStatement("SELECT COUNT(*) FROM vertrag WHERE id = ?");
            checkVertrag.setInt(1, id);
            ResultSet rs1 = checkVertrag.executeQuery();
            if (rs1.next() && rs1.getInt(1) > 0) {
                throw new VertragExistiertBereitsException(id);
            }

            // PrÃ¼fe: Produkt existiert?
            PreparedStatement checkProdukt = useConnection().prepareStatement("SELECT COUNT(*) FROM produkt WHERE id = ?");
            checkProdukt.setInt(1, produktId);
            ResultSet rs2 = checkProdukt.executeQuery();
            if (rs2.next() && rs2.getInt(1) == 0) {
                throw new ProduktExistiertNichtException(produktId);
            }

            // PrÃ¼fe: Kunde existiert?
            PreparedStatement checkKunde = useConnection().prepareStatement("SELECT COUNT(*) FROM kunde WHERE id = ?");
            checkKunde.setInt(1, kundenId);
            ResultSet rs3 = checkKunde.executeQuery();
            if (rs3.next() && rs3.getInt(1) == 0) {
                throw new KundeExistiertNichtException(kundenId);
            }

            // Versicherungsende berechnen
            LocalDate ende = versicherungsbeginn.plusYears(1).minusDays(1);

            // Insert Vertrag
            PreparedStatement insertStmt = useConnection().prepareStatement(
                    "INSERT INTO vertrag (id, Produkt_FK, Kunde_FK, versicherungsbeginn, versicherungsende) VALUES (?, ?, ?, ?, ?)");
            insertStmt.setInt(1, id);
            insertStmt.setInt(2, produktId);
            insertStmt.setInt(3, kundenId);
            insertStmt.setDate(4, java.sql.Date.valueOf(versicherungsbeginn));
            insertStmt.setDate(5, java.sql.Date.valueOf(ende));
            insertStmt.executeUpdate();

        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public BigDecimal calcMonatsrate(Integer vertragsId) {
        try {
            // Versicherungsbeginn holen
            PreparedStatement beginnStmt = useConnection().prepareStatement(
                    "SELECT versicherungsbeginn FROM vertrag WHERE id = ?");
            beginnStmt.setInt(1, vertragsId);
            ResultSet  rsBeginn = beginnStmt.executeQuery();

            if (!rsBeginn.next()) {
                throw new de.htwberlin.dbtech.exceptions.VertragExistiertNichtException(vertragsId);
            }

            LocalDate beginn = rsBeginn.getDate("versicherungsbeginn").toLocalDate();

            // Summiere Deckungspreise basierend auf gÃ¼ltigem Zeitraum und Deckungsbetrag
            PreparedStatement preisStmt = useConnection().prepareStatement(
                    "SELECT dp.preis FROM deckungspreis dp " +
                            "JOIN deckungsbetrag db ON dp.deckungsbetrag_fk = db.id " +
                            "JOIN deckung d ON d.deckungsart_fk = db.deckungsart_fk AND d.deckungsbetrag = db.deckungsbetrag " +
                            "WHERE d.vertrag_fk = ? AND ? BETWEEN dp.gueltig_von AND dp.gueltig_bis");
            preisStmt.setInt(1, vertragsId);
            preisStmt.setDate(2, java.sql.Date.valueOf(beginn));
            ResultSet rsPreise = preisStmt.executeQuery();

            BigDecimal summe = BigDecimal.ZERO;
            while (rsPreise.next()) {
                summe = summe.add(rsPreise.getBigDecimal("preis"));
            }

            return summe;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

}