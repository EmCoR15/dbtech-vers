package de.htwberlin.dbtech.aufgaben.dao;

import java.math.BigDecimal;

public interface IDeckungsartDao {
    boolean deckungsartExists(int deckungsartId);
    boolean passtZumVertrag(int vertragId, int deckungsartId);
    boolean isDeckungsbetragGueltig(int deckungsartId, BigDecimal betrag);
    boolean isPreisVorhanden(int deckungsartId, BigDecimal betrag);
}

