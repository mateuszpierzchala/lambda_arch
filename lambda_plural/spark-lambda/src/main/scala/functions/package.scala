import com.twitter.algebird.{HLL, HyperLogLog, HyperLogLogMonoid}
import domain.ActivityByProduct
import org.apache.spark.streaming.State

package object functions {

  def mapActivityStateFunc = (k: (String, Long), v: Option[ActivityByProduct], state: State[(Long, Long, Long)]) => {
    //bedzie służyc do akumulowania wartosci ilosci zakupów, dodan do koszyka i wyswietlen stron
    //poprzez iteracje przez wszystkie rekordy pobrane z ActivityByProduct
    //pobiera krotke jako klucz (nazwa produktu, timestamp) i wartosci ( reszta z ActivityByProduct czyli (nazwa, timestamp, p_c, a_t_c, p_v_c) i zwraca stan: krotke Longow czyli sumy p_c, a_t_c, p_v_c

    //zwraca poprzednie wartosci purchase_count.... lub zera jesli jeszcze nie istnieja do krot
    var (purchase_count, add_to_cart_count, page_view_count) = state.getOption().getOrElse((0L,0L,0L))
    //tworzy nowe wartosci na podstawie wartosci z value z pobranego z ActivityByProduct (jesli odczyta to ok jesli nie zapisuje zera
    val newVal = v  match{
      case Some(a: ActivityByProduct) => (a.purchase_count, a.add_to_cart_count, a.page_view_count)
      case _ => (0L, 0L, 0L)
    }
    //dodaje kolejne wartosci do p_c, a_t_c, p_v_c
    purchase_count += newVal._1
    add_to_cart_count += newVal._2
    page_view_count += newVal._3

    //update stanu p_c, a_t_c, p_v_c
    state.update((purchase_count, add_to_cart_count, page_view_count))
    // definicja underExposed (do uzycia pozniej)
    val underExposed = {
      if (purchase_count == 0)
        0
      else
        page_view_count/purchase_count
    }
    underExposed


  }

  def mapVisitorsStateFunc = (k: (String, Long), v: Option[HLL], state: State[HLL]) => {
      /*funkcja sluzaca do aproksymacji ilosci unikalnych uzytkowników odwiedzajacych strone za pomoca HyperLogLog
      dzięki użyciu HyperLogLog (HLL) nie musimy przetrzymywac w pamieci informacji o kazdym rekordzie co wpływa dodatnio na
        wydoajność aplikacji, obiekty HLL są w postaci macierzy i maja zdefiniowane przeładowanie funkcji '+' więc mozna je prosto sumowac*/

    //nowa wartosc currentVisitorHLL pobierana ze stanu jesli istnieje jesli nie to twrzona na nowo z iloscia bitow podana (12)
    val currentVisitorHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
    // nowa wartosc HLL dla nowego odwiedzajacego
    val newVisitorHLL = v match  {
        // jesli istnieje juz jakies HLL to newVisitor HLL jest suma poprzednich i aktualnego
      case Some( visitorHLL ) => currentVisitorHLL + visitorHLL
        //jesli nie istnieje to podstawa bedzie currentVisitorHLL pobrany lub utworzony na podstawie stanu
      case None => currentVisitorHLL
    }
    state.update(newVisitorHLL)
    val output = newVisitorHLL.approximateSize.estimate
    output
}}
