package de.htwberlin.dbtech.aufgaben.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeckungDao implements IDeckungDao {
    private final Connection connection;

    public DeckungDao(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void insertDeckung(int vertragId, int deckungsartId, BigDecimal betrag) {
        String sql = "INSERT INTO deckung (vertrag_fk, deckungsart_fk, deckungsbetrag) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, vertragId);
            ps.setInt(2, deckungsartId);
            ps.setBigDecimal(3, betrag);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Fehler beim Einfügen der Deckung", e);
        }
    }
}
