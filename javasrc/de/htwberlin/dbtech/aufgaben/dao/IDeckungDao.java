package de.htwberlin.dbtech.aufgaben.dao;

import java.math.BigDecimal;

public interface IDeckungDao {
    void insertDeckung(int vertragId, int deckungsartId, BigDecimal betrag);

}
