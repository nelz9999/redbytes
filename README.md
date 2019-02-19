# redbytes
A Go based utility for using Redis as a streaming file server. (In this case "streaming" means being able to retrieve the partially uploaded sequence of bytes before the full set of bytes is done being written.)

# History
While working at Orion Labs, I did an initial open-source implementation of a project similar to this one. After a third-party contractor accidentally made some licensed software public for a short while, we un-public-ed many of our repos in an overabundance of caution.

So, I thought I'd take another crack at it. See what I've learned in the meantime. To borrow a concept from martial arts, this project has sort of become my kata.

Only after starting in on *this* implementation did I find that someone had actually forked the original open source implementation, which is found here: https://github.com/adnaan/go-redis-bytestream. Since it was open and available, I've forked that into my own Github space, and frozen the repo for posterity: https://github.com/nelz9999/go-redis-bytestream.
