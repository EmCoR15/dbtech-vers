package de.htwberlin.dbtech.aufgaben.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeckungsartDao implements IDeckungsartDao {
    private final Connection connection;

    public DeckungsartDao(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean deckungsartExists(int deckungsartId) {
        try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM deckungsart WHERE id = ?")) {
            ps.setInt(1, deckungsartId);
            ResultSet rs = ps.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Fehler beim Prüfen der Deckungsart", e);
        }
    }

    @Override
    public boolean passtZumVertrag(int vertragId, int deckungsartId) {
        String sql = """
        SELECT COUNT(*) 
        FROM vertrag v
        JOIN deckungsart d ON v.produkt_fk = d.produkt_fk
        WHERE v.id = ? AND d.id = ?
    """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, vertragId);
            ps.setInt(2, deckungsartId);
            ResultSet rs = ps.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Fehler bei passtZumVertrag", e);
        }
    }

    @Override
    public boolean isDeckungsbetragGueltig(int deckungsartId, BigDecimal betrag) {
        String sql = """
        SELECT COUNT(*) 
        FROM deckungsbetrag 
        WHERE deckungsart_fk = ? AND deckungsbetrag = ?
    """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            ps.setBigDecimal(2, betrag);
            ResultSet rs = ps.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Fehler bei isDeckungsbetragGueltig", e);
        }
    }

    @Override
    public boolean isPreisVorhanden(int deckungsartId, BigDecimal betrag) {
        String sql = """
        SELECT COUNT(*) 
        FROM deckungspreis p
        JOIN deckungsbetrag db ON p.deckungsbetrag_fk = db.id
        WHERE db.deckungsart_fk = ? 
          AND db.deckungsbetrag = ? 
          AND p.gueltig_bis >= SYSDATE
    """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            ps.setBigDecimal(2, betrag);
            ResultSet rs = ps.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Fehler bei isPreisVorhanden", e);
        }
    }

}