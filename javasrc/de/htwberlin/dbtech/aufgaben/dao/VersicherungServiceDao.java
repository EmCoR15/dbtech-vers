package de.htwberlin.dbtech.aufgaben.dao;

import de.htwberlin.dbtech.aufgaben.ue03.IVersicherungService;
import de.htwberlin.dbtech.exceptions.*;

import java.math.BigDecimal;
import java.sql.Connection;

public class VersicherungServiceDao implements IVersicherungService {
    private Connection connection;
    private IDeckungDao deckungDao;
    private IVertragDao vertragDao;
    private IDeckungsartDao deckungsartDao;

    public void setConnection(Connection connection) {
        this.connection = connection;
        this.deckungDao = new DeckungDao(connection);
        this.vertragDao = new VertragDao(connection);
        this.deckungsartDao = new DeckungsartDao(connection);
    }

    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        if (!vertragDao.vertragExists(vertragsId)) {
            throw new VertragExistiertNichtException(vertragsId);
        }
        if (!deckungsartDao.deckungsartExists(deckungsartId)) {
            throw new DeckungsartExistiertNichtException(deckungsartId);
        }
        if (!deckungsartDao.passtZumVertrag(vertragsId, deckungsartId)) {
            throw new DeckungsartPasstNichtZuProduktException(vertragsId, deckungsartId);
        }
        if (!deckungsartDao.isDeckungsbetragGueltig(deckungsartId, deckungsbetrag)) {
            throw new UngueltigerDeckungsbetragException(deckungsbetrag);
        }
        if (!deckungsartDao.isPreisVorhanden(deckungsartId, deckungsbetrag)) {
            throw new DeckungspreisNichtVorhandenException(deckungsbetrag);
        }
        if (!vertragDao.isDeckungRegelkonform(vertragsId, deckungsartId, deckungsbetrag)) {
            throw new DeckungsartNichtRegelkonformException(deckungsartId);
        }

        deckungDao.insertDeckung(vertragsId, deckungsartId, deckungsbetrag);
    }
    }

