package de.htwberlin.dbtech.aufgaben.dao;

import java.math.BigDecimal;

public interface IVertragDao {
    boolean vertragExists(int vertragId);
    boolean isDeckungRegelkonform(int vertragId, int deckungsartId, BigDecimal betrag);

}
