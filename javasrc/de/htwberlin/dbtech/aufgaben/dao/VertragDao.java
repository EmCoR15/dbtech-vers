package de.htwberlin.dbtech.aufgaben.dao;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.Period;

public class VertragDao implements IVertragDao {

        private final Connection connection;

        public VertragDao(Connection connection) {
            this.connection = connection;
        }

        @Override
        public boolean vertragExists(int vertragId) {
            try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM vertrag WHERE id = ?")) {
                ps.setInt(1, vertragId);
                ResultSet rs = ps.executeQuery();
                return rs.next() && rs.getInt(1) > 0;
            } catch (SQLException e) {
                throw new RuntimeException("Fehler beim Prüfen des Vertrags", e);
            }
        }
    @Override
    public boolean isDeckungRegelkonform(int vertragId, int deckungsartId, BigDecimal betrag) {
        String sql = """
        SELECT r.r_betrag, r.r_alter, k.geburtsdatum
        FROM vertrag v
        JOIN kunde k ON v.kunde_fk = k.id
        JOIN ablehnungsregel r ON r.deckungsart_fk = ?
        WHERE v.id = ?
    """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, deckungsartId);
            ps.setInt(2, vertragId);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String rBetrag = rs.getString("r_betrag");
                String rAlter = rs.getString("r_alter");
                Date geburtsdatum = rs.getDate("geburtsdatum");

                int alter = calculateAge(geburtsdatum.toLocalDate());

                boolean betragErfuellt = evaluateCondition(rBetrag, betrag);
                boolean alterErfuellt = evaluateCondition(rAlter, new BigDecimal(alter));

                if (betragErfuellt && alterErfuellt) {
                    return false; // Nicht regelkonform
                }
            }
            return true; // Alles regelkonform
        } catch (SQLException e) {
            throw new RuntimeException("Fehler bei isDeckungRegelkonform", e);
        }
    }

    private int calculateAge(LocalDate geburtsdatum) {
        return Period.between(geburtsdatum, LocalDate.now()).getYears();
    }

    private boolean evaluateCondition(String condition, BigDecimal value) {
        if (condition == null || condition.trim().equals("--") || condition.trim().isEmpty()) {
            return false; // keine Bedingung ⇒ kein Ablehnungsgrund
        }

        String operator = condition.replaceAll("[0-9]", "").trim();
        String zahlTeil = condition.replaceAll("[^0-9]", "").trim();

        if (zahlTeil.isEmpty()) return false;

        BigDecimal vergleichswert = new BigDecimal(zahlTeil);

        return switch (operator) {
            case "=" -> value.compareTo(vergleichswert) == 0;
            case "!=" -> value.compareTo(vergleichswert) != 0;
            case "<" -> value.compareTo(vergleichswert) < 0;
            case "<=" -> value.compareTo(vergleichswert) <= 0;
            case ">" -> value.compareTo(vergleichswert) > 0;
            case ">=" -> value.compareTo(vergleichswert) >= 0;
            default -> false;
        };
    }

    }

