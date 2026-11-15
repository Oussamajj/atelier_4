package ma.oussama.kafka.events;

import java.util.Date;

/**
 * Événement de visite d'une page : nom de la page, utilisateur, date et durée (ms).
 */
public record PageEvent(String name, String user, Date date, long duration) {
}
