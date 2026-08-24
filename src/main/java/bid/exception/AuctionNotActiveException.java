package bid.exception;

public class AuctionNotActiveException extends RuntimeException {
    public AuctionNotActiveException(Long auctionId) {
        super("Auction " + auctionId + " is not currently active");
    }
}
